#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Doris MCP SSE Server Implementation

Implements a standard MCP SSE server based on MCP's SseServerTransport,
supports bidirectional communication with clients, and integrates with the existing Doris-MCP-Server.
"""

import asyncio
import json
import uuid
import logging
import time
from typing import Any, Optional
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse

# Get logger
logger = logging.getLogger("doris-mcp-sse")

class DorisMCPSseServer:
    """Doris MCP SSE Server Implementation"""
    
    def __init__(self, mcp_server, app: FastAPI):
        """
        Initialize the Doris MCP SSE server
        
        Args:
            mcp_server: FastMCP server instance
            app: FastAPI application instance
        """
        self.mcp_server = mcp_server
        
        # Ensure app is a FastAPI instance
        if not isinstance(app, FastAPI):
            logger.warning("Passed application is not a FastAPI instance, will use the existing FastAPI instance")
        
        self.app = app
        
        # Add CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["http://localhost:3100"],  # Specify frontend domain
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
            expose_headers=["*"]
        )
        
        # Client session management
        self.client_sessions = {}
        
        # Set up SSE routes
        self.setup_sse_routes()
        
        # Register startup event
        @self.app.on_event("startup")
        async def startup_event():
            # Start session cleanup task
            asyncio.create_task(self.cleanup_idle_sessions())
            # Start task to send periodic status updates
            asyncio.create_task(self.send_periodic_updates())
    
    def setup_sse_routes(self):
        """Set up SSE related routes"""
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint"""
            try:
                # Use direct health check logic
                return {
                    "status": "healthy",
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "server": "Doris MCP Server"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "error": str(e),
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                }
        
        @self.app.get("/status")
        async def status():
            """Get server status"""
            try:
                # Get tool list
                tools = await self.mcp_server.list_tools()
                tool_names = [tool.name if hasattr(tool, 'name') else str(tool) for tool in tools]
                logger.info(f"Getting tool list, currently registered tools: {tool_names}")
                
                # Get resource list
                resources = await self.mcp_server.list_resources()
                resource_names = [res.name if hasattr(res, 'name') else str(res) for res in resources]
                
                # Get prompt template list
                prompts = await self.mcp_server.list_prompts()
                prompt_names = [prompt.name if hasattr(prompt, 'name') else str(prompt) for prompt in prompts]
                
                return {
                    "status": "running",
                    "name": self.mcp_server.name,
                    "mode": "mcp_sse",
                    "clients": len(self.client_sessions),
                    "tools": tool_names,
                    "resources": resource_names,
                    "prompts": prompt_names
                }
            except Exception as e:
                logger.error(f"Error getting status: {str(e)}")
                return {
                    "status": "error",
                    "error": str(e)
                }
        
        @self.app.get("/sse")
        async def mcp_sse_init(request: Request):
            """SSE service entry point, establishes client connection (New Endpoint)"""
            # Generate session ID
            session_id = str(uuid.uuid4())
            logger.info(f"New SSE connection [Session ID: {session_id}] at /sse")
            
            # Create client session
            self.client_sessions[session_id] = {
                "client_id": request.headers.get("X-Client-ID", f"client_{str(uuid.uuid4())[:8]}"),
                "created_at": time.time(),
                "last_active": time.time(),
                "queue": asyncio.Queue()
            }
            
            # Immediately put endpoint information into the queue
            endpoint_data = f"/mcp/messages?session_id={session_id}"
            await self.client_sessions[session_id]["queue"].put({
                "event": "endpoint",
                "data": endpoint_data
            })
            
            # Create event generator
            async def event_generator():
                try:
                    while True:
                        # Use timeout to get new messages, to detect client disconnect
                        try:
                            message = await asyncio.wait_for(
                                self.client_sessions[session_id]["queue"].get(),
                                timeout=30
                            )
                            
                            # Check if it's a close command
                            if isinstance(message, dict) and message.get("event") == "close":
                                logger.info(f"Received close command [Session ID: {session_id}]")
                                break
                            
                            # Return message
                            if isinstance(message, dict):
                                if "event" in message:
                                    # If event field exists, it's a system event
                                    event_type = message["event"]
                                    event_data = message["data"]
                                    yield {
                                        "event": event_type,
                                        "data": event_data
                                    }
                                else:
                                    # Otherwise it's a normal message, use message event
                                    yield {
                                        "event": "message",
                                        "data": json.dumps(message)
                                    }
                            elif isinstance(message, str):
                                # If it's a string, send directly
                                yield {
                                    "event": "message",
                                    "data": message
                                }
                            else:
                                # Other types, convert to JSON
                                yield {
                                    "event": "message",
                                    "data": json.dumps(message)
                                }
                        except asyncio.TimeoutError:
                            # Send ping to keep connection alive
                            yield {
                                "event": "ping",
                                "data": "keepalive"
                            }
                            continue
                except asyncio.CancelledError:
                    # Connection cancelled
                    logger.info(f"SSE connection cancelled [Session ID: {session_id}]")
                except Exception as e:
                    # Other error occurred
                    logger.error(f"SSE event generator error [Session ID: {session_id}]: {str(e)}")
                finally:
                    # Clean up session
                    if session_id in self.client_sessions:
                        logger.info(f"Cleaning up session [Session ID: {session_id}]")
                        del self.client_sessions[session_id]
            
            # Return standard SSE response
            return EventSourceResponse(
                event_generator(),
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Credentials": "true",
                    "Access-Control-Allow-Methods": "*",
                    "Access-Control-Allow-Headers": "*",
                    "Access-Control-Expose-Headers": "*",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive"
                }
            )
        
        @self.app.options("/mcp/messages")
        async def mcp_messages_options(request: Request):
            """Handle preflight requests"""
            return JSONResponse(
                {},
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Credentials": "true",
                    "Access-Control-Allow-Methods": "*",
                    "Access-Control-Allow-Headers": "*",
                    "Access-Control-Expose-Headers": "*"
                }
            )
        
        @self.app.post("/mcp/messages")
        async def mcp_messages_handler(request: Request):
            """Handle client message requests, using class method"""
            return await self.mcp_message(request)
    
    async def cleanup_idle_sessions(self):
        """Clean up idle client sessions"""
        while True:
            await asyncio.sleep(60)  # Check every minute
            current_time = time.time()
            
            # Find sessions idle for over 5 minutes
            idle_sessions = []
            for session_id, session in self.client_sessions.items():
                if current_time - session["last_active"] > 300:  # 5 minutes
                    idle_sessions.append(session_id)
            
            # Close and remove idle sessions
            for session_id in idle_sessions:
                try:
                    # Send close message
                    await self.client_sessions[session_id]["queue"].put({"event": "close"})
                    # Clean up session
                    logger.info(f"Cleaned up idle session: {session_id}")
                except Exception as e:
                    logger.error(f"Error cleaning up session: {str(e)}")
                finally:
                    # Ensure session is removed
                    if session_id in self.client_sessions:
                        del self.client_sessions[session_id]
    
    async def send_periodic_updates(self):
        """Periodically send status updates to all clients"""
        while True:
            try:
                # Send status update every 5 seconds
                await asyncio.sleep(5)
                
                # If no clients are connected, skip this update
                if not self.client_sessions:
                    continue
                
                # Get current status
                status_data = {
                    "timestamp": time.time(),
                    "clients_count": len(self.client_sessions),
                    "server_status": "running"
                }
                
                # Send status update to all clients
                await self.broadcast_status_update(status_data)
            except Exception as e:
                logger.error(f"Error sending periodic updates: {str(e)}")
                # Wait a bit after error before continuing
                await asyncio.sleep(1)
    
    async def broadcast_status_update(self, status_data):
        """Broadcast status update to all clients
        
        Args:
            status_data: Status data
        """
        logger.debug(f"Broadcasting status update: {status_data}")
        message = {
            "jsonrpc": "2.0",
            "method": "notifications/status",
            "params": {
                "type": "status_update",
                "data": status_data
            }
        }
        await self.broadcast_message(message)
    
    async def broadcast_visualization_data(self, visualization_data):
        """Broadcast visualization data to all clients
        
        Args:
            visualization_data: Visualization data, should include type field
        """
        if not visualization_data or not isinstance(visualization_data, dict) or "type" not in visualization_data:
            logger.warning(f"Invalid visualization data: {visualization_data}")
            return
        
        logger.info(f"Broadcasting visualization data: {visualization_data['type']}")
        message = {
            "jsonrpc": "2.0",
            "method": "notifications/visualization",
            "params": {
                "type": "visualization",
                "data": visualization_data
            }
        }
        await self.broadcast_message(message)
    
    async def send_visualization_data(self, session_id, visualization_data):
        """Send visualization data to a specific client
        
        Args:
            session_id: Session ID
            visualization_data: Visualization data, should include type field
        """
        if not visualization_data or not isinstance(visualization_data, dict) or "type" not in visualization_data:
            logger.warning(f"Invalid visualization data: {visualization_data}")
            return
        
        if session_id not in self.client_sessions:
            logger.warning(f"Session does not exist: {session_id}")
            return
        
        logger.info(f"Sending visualization data to session {session_id}: {visualization_data['type']}")
        message = {
            "jsonrpc": "2.0",
            "method": "notifications/visualization",
            "params": {
                "type": "visualization",
                "data": visualization_data
            }
        }
        await self.client_sessions[session_id]["queue"].put(message)
    
    async def send_tool_result(self, session_id, tool_name, result_data, is_final=True):
        """Send tool execution result to the client
        
        Args:
            session_id: Session ID
            tool_name: Tool name
            result_data: Result data
            is_final: Whether it is the final result
        """
        if session_id not in self.client_sessions:
            logger.warning(f"Session does not exist: {session_id}")
            return
        
        logger.info(f"Sending tool result to session {session_id}: {tool_name}")
        message = {
            "jsonrpc": "2.0",
            "method": "notifications/tool_result",
            "params": {
                "type": "tool_result",
                "tool": tool_name,
                "result": result_data,
                "is_final": is_final
            }
        }
        await self.client_sessions[session_id]["queue"].put(message)
    
    async def broadcast_message(self, message):
        """Broadcast a message to all active sessions
        
        Args:
            message: Message to broadcast
        """
        # If no clients are connected, return immediately
        if not self.client_sessions:
            return
        
        # Create a copy of the session ID list so the original dictionary can be safely modified during iteration
        session_ids = list(self.client_sessions.keys())
        
        # Send message to all sessions
        for session_id in session_ids:
            try:
                if session_id in self.client_sessions:  # Check again, as session might have been removed during iteration
                    await self.client_sessions[session_id]["queue"].put(message)
            except Exception as e:
                logger.error(f"Error sending message to session {session_id}: {str(e)}")
    
    async def get_status(self):
        """Get server status"""
        return {
            "status": "running",
            "name": self.mcp_server.name,
            "mode": "mcp_sse",
            "clients": len(self.client_sessions)
        }

    async def mcp_message(self, request: Request):
        """Endpoint to receive client messages"""
        try:
            # Parse request parameters
            session_id = self._get_session_id(request)
            
            # Check if session exists
            if not session_id or session_id not in self.client_sessions:
                logger.warning(f"Session does not exist: {session_id}")
                return JSONResponse(
                    {"jsonrpc": "2.0", "error": {"code": -32000, "message": "Session does not exist or has expired"}},
                    status_code=401
                )
            
            # Update session last active time
            self.client_sessions[session_id]["last_active"] = time.time()
            
            # Get request body
            try:
                body = await request.json()
                logger.info(f"Received message [Session ID: {session_id}]: {json.dumps(body)}")
                
                # Process message
                message_id = body.get("id", str(uuid.uuid4()))
                
                # Handle JSON-RPC 2.0 formatted commands
                if "jsonrpc" not in body or body.get("jsonrpc") != "2.0" or "method" not in body:
                    return JSONResponse(
                        {"jsonrpc": "2.0", "id": message_id, "error": {"code": -32600, "message": "Invalid request, must be JSON-RPC 2.0 format"}},
                        status_code=400
                    )
                
                # Get method and parameters
                method = body.get("method")
                params = body.get("params", {})
                
                # Special handling for JSON-RPC formatted commands
                if method == "initialize":
                    # Initialization request
                    logger.info(f"Processing initialize command [Session ID: {session_id}]")
                    response = {
                        "jsonrpc": "2.0",
                        "id": message_id,
                        "result": {
                            "protocolVersion": "2024-11-05",
                            "name": self.mcp_server.name,
                            "instructions": "This is an MCP server for Apache Doris database",
                            "serverInfo": {
                                "version": "0.1.0",
                                "name": "Doris MCP Server"
                            },
                            "capabilities": {
                                "tools": {
                                    "supportsStreaming": True,
                                    "supportsProgress": True
                                },
                                "resources": {
                                    "supportsStreaming": False
                                },
                                "prompts": {
                                    "supported": True
                                }
                            }
                        }
                    }
                    await self.client_sessions[session_id]["queue"].put(response)
                    return JSONResponse(
                        {"status": "success"}, 
                        headers={
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Allow-Credentials": "true",
                            "Access-Control-Allow-Methods": "*",
                            "Access-Control-Allow-Headers": "*",
                            "Access-Control-Expose-Headers": "*"
                        }
                    )
                
                elif method == "mcp/listOfferings":
                    # List all available features
                    logger.info(f"Processing listOfferings command [Session ID: {session_id}]")
                    
                    # Get tool list
                    tools = await self.mcp_server.list_tools()
                    tools_json = [
                        {
                            "name": tool.name if hasattr(tool, "name") else str(tool),
                            "description": tool.description if hasattr(tool, "description") else "",
                            "inputSchema": tool.parameters if hasattr(tool, "parameters") else {
                                "type": "object",
                                "properties": {},
                                "required": []
                            }
                        }
                        for tool in tools
                    ]
                    
                    # Get resource list
                    resources = await self.mcp_server.list_resources()
                    resources_json = [res.model_dump() if hasattr(res, "model_dump") else res for res in resources]
                    
                    # Get prompt template list
                    prompts = await self.mcp_server.list_prompts()
                    prompts_json = [prompt.model_dump() if hasattr(prompt, "model_dump") else prompt for prompt in prompts]
                    
                    # Build response
                    response = {
                        "jsonrpc": "2.0",
                        "id": message_id,
                        "result": {
                            "tools": tools_json,
                            "resources": resources_json,
                            "prompts": prompts_json
                        }
                    }
                    await self.client_sessions[session_id]["queue"].put(response)
                    return JSONResponse(
                        {"status": "success"},
                        headers={
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Allow-Credentials": "true",
                            "Access-Control-Allow-Methods": "*",
                            "Access-Control-Allow-Headers": "*",
                            "Access-Control-Expose-Headers": "*"
                        }
                    )
                
                elif method == "mcp/listTools" or method == "tools/list":
                    # List all tools
                    logger.info(f"Processing listTools command [Session ID: {session_id}]")
                    tools = await self.mcp_server.list_tools()
                    tools_json = [
                        {
                            "name": tool.name if hasattr(tool, "name") else str(tool),
                            "description": tool.description if hasattr(tool, "description") else "",
                            "inputSchema": tool.parameters if hasattr(tool, "parameters") else {
                                "type": "object",
                                "properties": {},
                                "required": []
                            }
                        }
                        for tool in tools
                    ]
                    response = {
                        "jsonrpc": "2.0",
                        "id": message_id,
                        "result": {
                            "tools": tools_json
                        }
                    }
                    await self.client_sessions[session_id]["queue"].put(response)
                    return JSONResponse(
                        {"status": "success"},
                        headers={
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Allow-Credentials": "true",
                            "Access-Control-Allow-Methods": "*",
                            "Access-Control-Allow-Headers": "*",
                            "Access-Control-Expose-Headers": "*"
                        }
                    )
                
                elif method == "mcp/listResources":
                    # List all resources
                    logger.info(f"Processing listResources command [Session ID: {session_id}]")
                    resources = await self.mcp_server.list_resources()
                    resources_json = [res.model_dump() if hasattr(res, "model_dump") else res for res in resources]
                    response = {
                        "jsonrpc": "2.0",
                        "id": message_id,
                        "result": {
                            "resources": resources_json
                        }
                    }
                    await self.client_sessions[session_id]["queue"].put(response)
                    return JSONResponse(
                        {"status": "success"},
                        headers={
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Allow-Credentials": "true",
                            "Access-Control-Allow-Methods": "*",
                            "Access-Control-Allow-Headers": "*",
                            "Access-Control-Expose-Headers": "*"
                        }
                    )
                
                elif method == "mcp/callTool" or method == "tools/call":
                    # Call tool - special handling
                    tool_name = params.get("name")
                    arguments = params.get("arguments", {})
                    
                    if not tool_name:
                        error_response = {
                            "jsonrpc": "2.0",
                            "id": message_id,
                            "error": {
                                "code": -32602,
                                "message": "Invalid params: tool name is required"
                            }
                        }
                        await self.client_sessions[session_id]["queue"].put(error_response)
                        return JSONResponse(
                            {"status": "error", "message": "Tool name is required"},
                            status_code=400,
                            headers={
                                "Access-Control-Allow-Origin": "*",
                                "Access-Control-Allow-Credentials": "true",
                                "Access-Control-Allow-Methods": "*",
                                "Access-Control-Allow-Headers": "*",
                                "Access-Control-Expose-Headers": "*"
                            }
                        )
                    
                    # Get MCP instance
                    mcp = self.mcp_server
                    
                    # Check if it's a streaming tool call
                    stream_mode = "stream" in request.query_params or params.get("stream", False)
                    
                    logger.info(f"Calling tool [Session ID: {session_id}, Tool: {tool_name}, Args: {arguments}, Stream mode: {stream_mode}]")
                    
                    if stream_mode:
                        # Streaming tool call
                        logger.info(f"Using streaming response to handle tool call [Session ID: {session_id}, Tool: {tool_name}]")
                        
                        try:
                            # Find tool
                            tool_instance = None
                            for tool in await mcp.list_tools():
                                if getattr(tool, 'name', '') == tool_name:
                                    tool_instance = tool
                                    break
                            
                            if not tool_instance:
                                raise Exception(f"Tool {tool_name} does not exist")
                            
                            # Define callback function
                            async def callback(content, metadata):
                                # Send partial result
                                partial_message = {
                                    "jsonrpc": "2.0",
                                    "id": message_id,
                                    "partial": True,
                                    "result": {
                                        "content": content,
                                        "metadata": metadata
                                    }
                                }
                                # Put message into queue
                                await self.client_sessions[session_id]["queue"].put(partial_message)
                                
                                # If visualization data is included, broadcast to all clients
                                if metadata and "visualization" in metadata:
                                    await self.broadcast_visualization_data(metadata["visualization"])
                            
                            # Build argument dictionary
                            kwargs = dict(arguments)
                            kwargs['callback'] = callback
                            
                            # Execute tool call
                            # Fix: Do not call tool object directly, instead create async task to call call_tool method
                            # func = tool_instance.func if hasattr(tool_instance, 'func') else tool_instance
                            
                            # Start async task to execute streaming tool
                            # asyncio.create_task(self._execute_stream_tool(func, kwargs, message_id, session_id))
                            # Modified to use call_tool method
                            asyncio.create_task(self._execute_stream_tool_wrapper(tool_name, kwargs, message_id, session_id, request))
                            
                            # Return received confirmation
                            return JSONResponse(
                                {"status": "processing"}, 
                                headers={
                                    "Access-Control-Allow-Origin": "*",
                                    "Access-Control-Allow-Credentials": "true",
                                    "Access-Control-Allow-Methods": "*",
                                    "Access-Control-Allow-Headers": "*",
                                    "Access-Control-Expose-Headers": "*"
                                }
                            )
                        except Exception as e:
                            logger.error(f"Streaming tool processing error: {str(e)}")
                            error_message = {
                                "jsonrpc": "2.0",
                                "id": message_id,
                                "success": False,
                                "error": str(e)
                            }
                            # Put error message into queue
                            await self.client_sessions[session_id]["queue"].put(error_message)
                            return JSONResponse(
                                {"status": "error", "message": str(e)}, 
                                status_code=500,
                                headers={
                                    "Access-Control-Allow-Origin": "*",
                                    "Access-Control-Allow-Credentials": "true",
                                    "Access-Control-Allow-Methods": "*",
                                    "Access-Control-Allow-Headers": "*",
                                    "Access-Control-Expose-Headers": "*"
                                }
                            )
                    else:
                        # Non-streaming tool call
                        logger.info(f"Using standard response to handle tool call [Session ID: {session_id}, Tool: {tool_name}]")
                        
                        try:
                            # Find tool
                            tool_instance = None
                            for tool in await mcp.list_tools():
                                if getattr(tool, 'name', '') == tool_name:
                                    tool_instance = tool
                                    break
                            
                            if not tool_instance:
                                error_response = {
                                    "jsonrpc": "2.0",
                                    "id": message_id,
                                    "error": {
                                        "code": -32601,
                                        "message": f"Tool '{tool_name}' not found"
                                    }
                                }
                                await self.client_sessions[session_id]["queue"].put(error_response)
                                return JSONResponse(
                                    {"status": "error", "message": f"Tool '{tool_name}' not found"},
                                    status_code=404,
                                    headers={
                                        "Access-Control-Allow-Origin": "*",
                                        "Access-Control-Allow-Credentials": "true",
                                        "Access-Control-Allow-Methods": "*",
                                        "Access-Control-Allow-Headers": "*",
                                        "Access-Control-Expose-Headers": "*"
                                    }
                                )
                            
                            # Execute tool call
                            # Fix: Do not call tool object directly, instead call custom call_tool method
                            result = await self.call_tool(tool_name, arguments, request)
                            
                            # Special formatting for result
                            # If result is already in the correct format, use directly
                            if isinstance(result, dict) and "content" in result and isinstance(result["content"], list):
                                formatted_result = result
                            else:
                                # Otherwise, format into standard format
                                formatted_result = {
                                    "content": [
                                        {
                                            "type": "json", 
                                            "text": result if isinstance(result, str) else json.dumps(result, ensure_ascii=False)
                                        }
                                    ]
                                }
                            
                            # Build response
                            response = {
                                "jsonrpc": "2.0",
                                "id": message_id,
                                "result": formatted_result
                            }
                            
                            # Put response into queue
                            await self.client_sessions[session_id]["queue"].put(response)
                            
                            # Return received confirmation
                            return JSONResponse(
                                {"status": "success"}, 
                                headers={
                                    "Access-Control-Allow-Origin": "*",
                                    "Access-Control-Allow-Credentials": "true",
                                    "Access-Control-Allow-Methods": "*",
                                    "Access-Control-Allow-Headers": "*",
                                    "Access-Control-Expose-Headers": "*"
                                }
                            )
                        except Exception as e:
                            logger.error(f"Tool call error: {str(e)}", exc_info=True)
                            
                            # Build error response
                            if str(e).startswith('{"code":'):
                                # If it's a JSON formatted error, use directly
                                try:
                                    error_obj = json.loads(str(e))
                                    error_response = {
                                        "jsonrpc": "2.0",
                                        "id": message_id,
                                        "error": error_obj
                                    }
                                except:
                                    # If parsing fails, use standard format
                                    error_response = {
                                        "jsonrpc": "2.0",
                                        "id": message_id,
                                        "error": {
                                            "code": -32000,
                                            "message": str(e)
                                        }
                                    }
                            else:
                                # Normal error string
                                error_response = {
                                    "jsonrpc": "2.0",
                                    "id": message_id,
                                    "error": {
                                        "code": -32000,
                                        "message": str(e)
                                    }
                                }
                            
                            # Put error response into queue
                            await self.client_sessions[session_id]["queue"].put(error_response)
                            
                            # Return error status
                            return JSONResponse(
                                {"status": "error", "message": str(e)},
                                status_code=500,
                                headers={
                                    "Access-Control-Allow-Origin": "*",
                                    "Access-Control-Allow-Credentials": "true",
                                    "Access-Control-Allow-Methods": "*",
                                    "Access-Control-Allow-Headers": "*",
                                    "Access-Control-Expose-Headers": "*"
                                }
                            )
                else:
                    # Other message types, forward directly to MCP for handling
                    logger.info(f"Processing general message [Session ID: {session_id}]")
                    
                    try:
                        # Process message
                        # FastMCP object doesn't have process_message method, build response directly instead
                        # result = await mcp.process_message(body)
                        
                        # Build response
                        response = {
                            "jsonrpc": "2.0",
                            "id": message_id,
                            "result": {
                                "status": "ok",
                                "message": "Message received, but unable to process unrecognized message type"
                            }
                        }
                    except Exception as e:
                        logger.error(f"Error processing message [Session ID: {session_id}]: {str(e)}")
                        response = {
                            "jsonrpc": "2.0",
                            "id": message_id,
                            "error": {
                                "code": -32000,
                                "message": f"Error processing message: {str(e)}"
                            }
                        }

                # Put response into queue
                if response:
                    await self.client_sessions[session_id]["queue"].put(response)

                # Return received confirmation to HTTP request
                return JSONResponse(
                    {"status": "received"}, 
                    headers={
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Credentials": "true",
                        "Access-Control-Allow-Methods": "*",
                        "Access-Control-Allow-Headers": "*",
                        "Access-Control-Expose-Headers": "*"
                    }
                )
            except Exception as e:
                logger.error(f"Error processing message: {str(e)}")
                # Send error response
                error_response = {
                    "jsonrpc": "2.0",
                    "id": message_id if 'message_id' in locals() else str(uuid.uuid4()),
                    "error": {
                        "code": -32000,
                        "message": str(e)
                    }
                }
                
                await self.client_sessions[session_id]["queue"].put(error_response)
                return JSONResponse(
                    {"status": "error", "message": str(e)}, 
                    status_code=500,
                    headers={
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Credentials": "true",
                        "Access-Control-Allow-Methods": "*",
                        "Access-Control-Allow-Headers": "*",
                        "Access-Control-Expose-Headers": "*"
                    }
                )
        except Exception as e:
            logger.error(f"Error processing request: {str(e)}", exc_info=True)
            return JSONResponse(
                {
                    "jsonrpc": "2.0",
                    "id": "unknown",
                    "error": {
                        "code": -32000,
                        "message": f"Error processing request: {str(e)}"
                    }
                },
                status_code=500,
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Credentials": "true",
                    "Access-Control-Allow-Methods": "*",
                    "Access-Control-Allow-Headers": "*",
                    "Access-Control-Expose-Headers": "*"
                }
            )

    async def _execute_stream_tool_wrapper(self, tool_name, kwargs, message_id, session_id, request):
        """Wrapper for streaming tool calls
        
        Args:
            tool_name: Tool name
            kwargs: Function parameters
            message_id: Message ID
            session_id: Session ID
            request: Request object
        """
        try:
            # Execute by calling the standard tool method
            result = await self.call_tool(tool_name, kwargs, request)
            
            # Send completion message
            final_message = {
                "jsonrpc": "2.0",
                "id": message_id,
                "success": True,
                "result": result
            }
            
            # Check if session still exists
            if session_id in self.client_sessions:
                await self.client_sessions[session_id]["queue"].put(final_message)
                logger.info(f"Streaming tool execution completed [Session ID: {session_id}, Message ID: {message_id}]")
            else:
                logger.warning(f"Streaming tool execution completed but session closed [Session ID: {session_id}]")
        except Exception as e:
            logger.error(f"Streaming tool execution failed [Session ID: {session_id}]: {str(e)}")
            
            # Send error message
            error_message = {
                "jsonrpc": "2.0",
                "id": message_id,
                "success": False,
                "error": str(e)
            }
            
            # Check if session still exists
            if session_id in self.client_sessions:
                await self.client_sessions[session_id]["queue"].put(error_message)
            else:
                logger.warning(f"Streaming tool execution failed but session closed [Session ID: {session_id}]") 

    async def broadcast_tool_result(self, tool_name, result_data):
        """Broadcast tool call result to all clients
        
        Args:
            tool_name: Tool name
            result_data: Result data
        """
        logger.info(f"Broadcasting tool result: {tool_name}")
        message = {
            "jsonrpc": "2.0",
            "method": "notifications/tool_result",
            "params": {
                "type": "tool_result",
                "tool": tool_name,
                "result": result_data
            }
        }
        await self.broadcast_message(message)

    async def call_tool(self, tool_name, arguments, request):
        """
        Calls the specified tool and returns the result
        
        Args:
            tool_name: Tool name
            arguments: Tool parameters
            request: Original request object
        
        Returns:
            Tool call result
        """
        logger.info(f"Calling tool: {tool_name}, Parameters: {json.dumps(arguments, ensure_ascii=False)}")
        
        # Get recent query content, used to handle random_string parameter
        recent_query = self._extract_recent_query(request)
        
        # Handle tool name mapping - Add support for standard name tools
        tool_mapping = {
            # --- Retained/New Tools ---
            "exec_query": "mcp_doris_exec_query",
            "get_table_schema": "mcp_doris_get_table_schema",
            "get_db_table_list": "mcp_doris_get_db_table_list",
            "get_db_list": "mcp_doris_get_db_list",
            "get_table_comment": "mcp_doris_get_table_comment",
            "get_table_column_comments": "mcp_doris_get_table_column_comments",
            "get_table_indexes": "mcp_doris_get_table_indexes",
            "get_recent_audit_logs": "mcp_doris_get_recent_audit_logs"
        }
        
        # If it's a standard name, convert to MCP name
        mapped_tool_name = tool_mapping.get(tool_name, tool_name)
        
        # Import tool functions from mcp_doris_tools
        try:
            # Import tool module
            import doris_mcp_server.tools.mcp_doris_tools as mcp_tools
            
            # Get the corresponding tool function
            tool_function = getattr(mcp_tools, mapped_tool_name, None)
            
            if not tool_function:
                # If it doesn't exist in mcp_tools, try getting the tool using the MCP server instance
                mcp = self.mcp_server
                # Find the corresponding tool
                for tool in await mcp.list_tools():
                    if getattr(tool, 'name', '') == mapped_tool_name:
                        tool_function = tool
                        break
                
                if not tool_function:
                    raise ValueError(f"Tool not found: {tool_name} / {mapped_tool_name}")
            
            # Process common input parameter conversions
            processed_args = self._process_tool_arguments(mapped_tool_name, arguments, recent_query)
            
            # Call the tool function
            try:
                # Log tool type and attribute information to help debugging
                logger.debug(f"Tool function type: {type(tool_function)}")
                logger.debug(f"Tool function attributes: {dir(tool_function)}")
                
                if callable(tool_function):
                    logger.debug("Tool function is callable, calling directly")
                    result = await tool_function(**processed_args)
                elif hasattr(tool_function, 'run'):
                    logger.debug("Tool function has run method, calling run method")
                    result = await tool_function.run(**processed_args)
                elif hasattr(tool_function, 'execute'):
                    logger.debug("Tool function has execute method, calling execute method")
                    result = await tool_function.execute(**processed_args)
                elif hasattr(tool_function, 'call'):
                    logger.debug("Tool function has call method, calling call method")
                    result = await tool_function.call(**processed_args)
                elif hasattr(tool_function, '__call__'):
                    logger.debug("Tool function has __call__ method, calling __call__ method")
                    result = await tool_function.__call__(**processed_args)
                else:
                    # If it's a dict type, try getting the function from it
                    if isinstance(tool_function, dict) and 'function' in tool_function:
                        logger.debug("Tool is a dictionary type, trying to get 'function' key")
                        actual_func = tool_function['function']
                        if callable(actual_func):
                            result = await actual_func(**processed_args)
                        else:
                            raise ValueError(f"Function in dictionary is not callable: {type(actual_func)}")
                    else:
                        raise ValueError(f"Unsupported tool type: {type(tool_function)}, Attributes: {dir(tool_function)}")
            except Exception as e:
                logger.error(f"Failed to call tool function: {str(e)}", exc_info=True)
                raise ValueError(f"Error calling tool: {str(e)}")
            
            # Return tool execution result
            return result
        except AttributeError as e:
            logger.error(f"Tool function does not exist: {mapped_tool_name}, Error: {str(e)}")
            raise ValueError(f"Tool function does not exist: {mapped_tool_name}")
        except Exception as e:
            logger.error(f"Error calling tool: {str(e)}", exc_info=True)
            raise ValueError(f"Error calling tool: {str(e)}")
    
    def _process_tool_arguments(self, tool_name, arguments, recent_query):
        """
        Process tool parameters, supporting special handling logic
        
        Args:
            tool_name: Tool name (MCP internal name, e.g., mcp_doris_...)
            arguments: Original parameters
            recent_query: Recent query content
            
        Returns:
            Processed parameter dictionary
        """
        # Copy parameters to avoid modifying the original object
        processed_args = dict(arguments)
        
        # Handle potential random_string parameter as fallback
        if "random_string" in processed_args and tool_name.startswith("mcp_doris_"):
            random_string = processed_args.pop("random_string", "")
            logger.debug(f"Processing random_string parameter for tool {tool_name}: '{random_string}'")

            # 1. For exec_query
            if tool_name == "mcp_doris_exec_query":
                if not processed_args.get("sql"):
                    sql_fallback = random_string or recent_query
                    if sql_fallback:
                        if not random_string and recent_query:
                             import re
                             sql_matches = re.findall(r'```sql\s*([\s\S]+?)\s*```', recent_query)
                             if sql_matches:
                                 sql_fallback = sql_matches[0].strip()
                        
                        if sql_fallback:
                            logger.info(f"Using random_string/recent_query as SQL for exec_query: {sql_fallback[:100]}...")
                            processed_args["sql"] = sql_fallback
                        else:
                             logger.warning(f"exec_query missing sql parameter, and random_string/recent_query is empty or SQL cannot be extracted")
                    else:
                         logger.warning(f"exec_query missing sql parameter, and both random_string and recent_query are empty")
            
            # 2. For tools requiring table_name
            elif tool_name in [
                "mcp_doris_get_table_schema", 
                "mcp_doris_get_table_comment", 
                "mcp_doris_get_table_column_comments", 
                "mcp_doris_get_table_indexes"
            ]:
                if not processed_args.get("table_name"):
                    table_fallback = random_string
                    if table_fallback:
                         logger.info(f"Using random_string/recent_query as table_name for {tool_name}: {table_fallback}")
                         processed_args["table_name"] = table_fallback
                    else:
                        logger.warning(f"{tool_name} missing table_name parameter, and random_string/recent_query is empty or table name cannot be extracted")
            
            # 3. Other tools
            else:
                logger.debug(f"Tool {tool_name} does not apply random_string fallback logic, or logic is undefined.")
        
        # Ensure return is outside the main if block
        return processed_args

    def _extract_recent_query(self, request: Request) -> Optional[str]:
        """
        Extract the most recent user query from the request
        
        Args:
            request: Request object
            
        Returns:
            Optional[str]: The most recent user query, or None if not found
        """
        try:
            # Try to extract message history from request body
            body = None
            body_bytes = getattr(request, "_body", None)
            if body_bytes:
                try:
                    body = json.loads(body_bytes)
                except:
                    pass
            
            if not body:
                body = getattr(request, "_json", {})
            
            # Find the most recent user message from message history
            messages = body.get("params", {}).get("messages", [])
            if messages:
                # Iterate messages in reverse to find the most recent user message
                for msg in reversed(messages):
                    if msg.get("role") == "user":
                        return msg.get("content", "")
            
            # If not found in message history, try extracting from the original message
            message = body.get("params", {}).get("message", {})
            if message and message.get("role") == "user":
                return message.get("content", "")
            
            return None
        except Exception as e:
            logger.error(f"Error extracting recent query: {str(e)}")
            return None

    def format_tool_result(self, result):
        """
        Format tool call result into a unified format
        
        Args:
            result: Original tool call result
        
        Returns:
            Formatted result
        """
        try:
            # If result is already a dictionary, return directly
            if isinstance(result, dict):
                return result
            
            # If it's a string, try parsing as JSON
            if isinstance(result, str):
                try:
                    return json.loads(result)
                except json.JSONDecodeError:
                    # Pure text result
                    return {"content": result}
            
            # If it's another type, convert to string
            return {"content": str(result)}
        
        except Exception as e:
            logger.error(f"Error formatting tool result: {str(e)}")
            return {"error": str(e)}

    def _get_session_id(self, request: Request) -> str:
        """
        Get session ID from the request
        
        Tries to get session ID from the following locations (in priority order):
        1. Query parameter session_id
        2. session_id field in request body
        3. X-Session-ID header
        
        Args:
            request: Request object
            
        Returns:
            str: Session ID, or None if not found
        """
        # Get from query parameter
        session_id = request.query_params.get("session_id")
        if session_id:
            return session_id
            
        # Try getting from request body
        try:
            body = getattr(request, "_json", None)
            if not body:
                body_bytes = getattr(request, "_body", None)
                if body_bytes:
                    try:
                        body = json.loads(body_bytes)
                    except:
                        pass
            
            if body and isinstance(body, dict) and "session_id" in body:
                return body["session_id"]
        except:
            pass
            
        # Get from request header
        session_id = request.headers.get("X-Session-ID")
        if session_id:
            return session_id
            
        return None
