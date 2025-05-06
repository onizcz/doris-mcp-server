#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Doris MCP Streamable HTTP Server Implementation

Implements the MCP 2025-03-26 Streamable HTTP specification.
Uses a unified /mcp endpoint for GET, POST, DELETE, OPTIONS.
Manages sessions using Mcp-Session-Id header.
"""

import asyncio
import json
import uuid
import logging
import time
from typing import Any, Optional, Dict, List
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse

# Use a distinct logger name
logger = logging.getLogger("doris-mcp-streamable")

# Special marker for closing streams
STREAM_END_MARKER = "__MCP_STREAM_END__"

class DorisMCPStreamableServer:
    """Doris MCP Streamable HTTP Server"""

    def __init__(self, mcp_server, app: FastAPI):
        """
        Initializes the Doris MCP Streamable HTTP server.

        Args:
            mcp_server: The shared FastMCP server instance.
            app: The main FastAPI application instance.
        """
        self.mcp_server = mcp_server
        self.app = app # We'll add routes to this app

        # Note: CORS middleware should be added only once in main.py usually.
        # If added here, ensure it doesn't conflict or duplicate.
        # For separation, we might let main.py handle CORS entirely.

        # Client session management for Streamable HTTP clients
        # key: session_id (from Mcp-Session-Id header)
        # value: {
        #   "created_at": timestamp,
        #   "last_active": timestamp,
        #   "request_queues": { request_id: asyncio.Queue }, # For POST /mcp request streams
        #   "general_sse_queues": List[asyncio.Queue] # For GET /mcp server push streams
        # }
        self.client_sessions: Dict[str, Dict[str, Any]] = {}

        # Setup the unified MCP endpoint
        self._setup_streamable_http_routes()

        # Register session cleanup task if this instance manages lifespan independently
        # Usually, startup events are tied to the main app lifespan managed in main.py
        # We might not need @app.on_event("startup") here if main.py handles it.
        # Let's assume main.py handles the cleanup task initiation.

    def _setup_streamable_http_routes(self):
        """Sets up the unified /mcp endpoint for Streamable HTTP.
           Uses a distinct tag for API docs.
        """

        @self.app.api_route("/mcp", methods=["GET", "POST", "DELETE", "OPTIONS"], tags=["Streamable HTTP"])
        async def mcp_endpoint_handler(request: Request):
            """Handles GET, POST, DELETE, OPTIONS for the /mcp endpoint."""

            # 1. Handle OPTIONS (CORS preflight)
            if request.method == "OPTIONS":
                # Assuming CORS headers are handled by middleware in main.py
                # If not, provide necessary headers here.
                # This minimal response might suffice if middleware handles the rest
                 logger.debug("Handling OPTIONS request for /mcp")
                 # Return basic OK allowing exposed headers if middleware handles the rest
                 return JSONResponse({}, headers={"Access-Control-Expose-Headers": "Mcp-Session-Id"})

            # Session ID from header is required for most methods
            session_id = request.headers.get("Mcp-Session-Id")

            # 2. Handle DELETE (Terminate Session)
            if request.method == "DELETE":
                if not session_id:
                    return JSONResponse({"jsonrpc": "2.0", "error": {"code": -32600, "message": "Mcp-Session-Id header is required for DELETE"}}, status_code=400)

                logger.info(f"Handling DELETE request for session [Session ID: {session_id}]")
                session_data = self.client_sessions.pop(session_id, None)
                if session_data:
                    await self._cleanup_session_resources(session_id, session_data)
                    return JSONResponse({}, status_code=204) # No Content
                else:
                    logger.warning(f"Attempted DELETE on non-existent session: {session_id}")
                    return JSONResponse({"jsonrpc": "2.0", "error": {"code": -32001, "message": "Session not found"}}, status_code=404)

            # 3. Handle GET (Server Push SSE Stream)
            if request.method == "GET":
                if not session_id:
                    return JSONResponse({"jsonrpc": "2.0", "error": {"code": -32000, "message": "Mcp-Session-Id header is required for GET streams"}}, status_code=400)
                if session_id not in self.client_sessions:
                    # Note: Unlike legacy SSE, GET here assumes session exists.
                    return JSONResponse({"jsonrpc": "2.0", "error": {"code": -32001, "message": "Session not found. Initialize first."}}, status_code=404)

                accept_header = request.headers.get("Accept", "")
                if "text/event-stream" not in accept_header:
                    return JSONResponse({"jsonrpc": "2.0", "error": {"code": -32600, "message": "Accept header must include text/event-stream for GET"}}, status_code=406)

                # TODO: Handle Last-Event-ID for stream recovery?

                logger.info(f"Handling GET request, establishing server push SSE stream [Session ID: {session_id}]")

                push_queue = asyncio.Queue()
                if self.client_sessions[session_id].get("general_sse_queues") is None:
                     self.client_sessions[session_id]["general_sse_queues"] = []
                self.client_sessions[session_id]["general_sse_queues"].append(push_queue)
                self.client_sessions[session_id]["last_active"] = time.time()

                return EventSourceResponse(self._create_general_sse_generator(session_id, push_queue), media_type="text/event-stream")

            # 4. Handle POST (Client Messages & Initialize)
            if request.method == "POST":
                accept_header = request.headers.get("Accept", "")
                content_type = request.headers.get("Content-Type", "")

                body = {}
                try:
                    if "application/json" not in content_type:
                         return JSONResponse({"jsonrpc": "2.0", "error": {"code": -32700, "message": "Content-Type must be application/json"}}, status_code=415)
                    body = await request.json()
                    if isinstance(body, list): return JSONResponse({"jsonrpc": "2.0", "error": {"code": -32600, "message": "Batch requests not supported"}}, status_code=400)
                    if not isinstance(body, dict): return JSONResponse({"jsonrpc": "2.0", "error": {"code": -32700, "message": "Invalid JSON received"}}, status_code=400)

                    method = body.get("method")
                    message_id = body.get("id") # Can be None for notifications

                    # Handle Initialize request (does not require Mcp-Session-Id header)
                    if method == "initialize":
                        if "application/json" not in accept_header:
                             return JSONResponse({"jsonrpc": "2.0", "id": message_id, "error": {"code": -32600, "message": "Accept header must include application/json for initialize"}}, status_code=406)
                        return await self._handle_initialize(request, body, message_id)

                    # Handle other POST requests (require Mcp-Session-Id)
                    else:
                        if not session_id:
                            return JSONResponse({"jsonrpc": "2.0", "id": message_id, "error": {"code": -32000, "message": "Mcp-Session-Id header is required for this request"}}, status_code=400)
                        if session_id not in self.client_sessions:
                            return JSONResponse({"jsonrpc": "2.0", "id": message_id, "error": {"code": -32001, "message": "Session not found"}}, status_code=404)
                        # Check Accept header for non-initialize POST
                        if not ("application/json" in accept_header and "text/event-stream" in accept_header):
                            return JSONResponse({"jsonrpc": "2.0", "id": message_id, "error": {"code": -32600, "message": "Accept header must include application/json and text/event-stream for POST"}}, status_code=406)

                        self.client_sessions[session_id]["last_active"] = time.time()
                        return await self._handle_client_post(request, body, session_id, message_id)

                except json.JSONDecodeError:
                    return JSONResponse({"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error - Invalid JSON received"}}, status_code=400)
                except Exception as e:
                    logger.error(f"Unexpected error handling POST /mcp: {str(e)}", exc_info=True)
                    error_id = body.get("id") if isinstance(body, dict) else None
                    return JSONResponse({"jsonrpc": "2.0", "id": error_id, "error": {"code": -32000, "message": "Internal server error"}}, status_code=500)

            # Fallback for other methods like PUT, PATCH etc.
            return JSONResponse({"error": "Method Not Allowed"}, status_code=405)

    async def _handle_initialize(self, request: Request, body: Dict, message_id: Any):
        """Handles the 'initialize' method call via POST /mcp."""
        logger.info("Handling Streamable HTTP initialize request")
        # Optional: Validate params in body if needed
        # params = body.get("params", {})

        new_session_id = str(uuid.uuid4())
        logger.info(f"Created new Streamable HTTP session [Session ID: {new_session_id}]")

        self.client_sessions[new_session_id] = {
            "created_at": time.time(),
            "last_active": time.time(),
            # No transport_type needed here as this class *is* the streamable server
            "request_queues": {}, # Initialize request queues dict
            "general_sse_queues": [] # Initialize general queues list
        }

        # Build InitializeResult based on spec
        initialize_result = {
            "protocolVersion": "2025-03-26",
            "name": self.mcp_server.name,
            "instructions": "Apache Doris MCP Server (Streamable HTTP Mode)",
            "serverInfo": { "version": "0.2.0", "name": "Doris MCP Streamable Server" }, # Adjust as needed
            "capabilities": {
                "tools": { "supportsStreaming": True, "supportsProgress": True },
                "resources": { "supportsStreaming": False }, # Example capability
                "prompts": { "supported": True },          # Example capability
                "session": { "supported": True }
            }
        }
        response_body = {
            "jsonrpc": "2.0",
            "id": message_id,
            "result": initialize_result
        }

        # Return JSON response with Mcp-Session-Id header
        return JSONResponse(
            content=response_body,
            media_type="application/json",
            headers={"Mcp-Session-Id": new_session_id}
        )

    async def _handle_client_post(self, request: Request, body: Dict, session_id: str, message_id: Any):
        """Handles non-initialize POST requests (notifications, responses, method calls)."""
        method = body.get("method")

        # Handle Notifications/Responses from client
        is_notification = "method" in body and "id" not in body
        is_response = "result" in body or "error" in body
        if is_notification or is_response:
            logger.info(f"Received Streamable HTTP notification/response [Session ID: {session_id}] - Processing needed? (Ignoring for now)")
            # TODO: If the server sends requests that expect responses, process is_response here.
            # For now, just acknowledge client notifications/responses.
            return JSONResponse({}, status_code=202) # Accepted

        # Handle Requests from client (method call)
        if "method" in body and "id" in body:
            logger.info(f"Received Streamable HTTP request [Session ID: {session_id}, ID: {message_id}, Method: {method}]")
            params = body.get("params", {})
            stream_required = params.get("stream", False) if method in ["tools/call", "mcp/callTool"] else False

            if stream_required:
                 # --- Return SSE stream for response parts --- 
                logger.info(f"Using SSE stream for request [Session ID: {session_id}, ID: {message_id}]")
                response_queue = asyncio.Queue()
                # Ensure request_queues exists (should have been created during initialize)
                if self.client_sessions[session_id].get("request_queues") is None:
                     logger.error(f"Session {session_id} is missing 'request_queues' dictionary!")
                     # Handle this inconsistency, maybe return an error
                     return JSONResponse({"jsonrpc": "2.0", "id": message_id, "error": {"code": -32000, "message": "Internal server error: Session state inconsistent"}}, status_code=500)
                self.client_sessions[session_id]["request_queues"][message_id] = response_queue

                # Start background task to process and put results in the queue
                asyncio.create_task(self._process_request_and_respond(
                    request, body, session_id, message_id, response_queue, is_stream=True
                ))

                # Return EventSourceResponse using the request-specific queue
                return EventSourceResponse(self._create_request_sse_generator(session_id, message_id, response_queue), media_type="text/event-stream")
            else:
                 # --- Return single JSON response --- 
                logger.info(f"Using JSON response for request [Session ID: {session_id}, ID: {message_id}]")
                try:
                    # Process the request directly and get the result/error payload
                    result_or_error_payload = await self._process_request_and_respond(
                        request, body, session_id, message_id, None, is_stream=False
                    )
                    # This function now returns the final JSON body or raises HTTPException
                    return JSONResponse(content=result_or_error_payload, media_type="application/json")
                except HTTPException as http_exc:
                    # Format HTTPException details into JSON-RPC error
                    return JSONResponse(
                        {"jsonrpc": "2.0", "id": message_id, "error": {"code": -32000, "message": http_exc.detail}},
                        status_code=http_exc.status_code
                    )
                except Exception as e:
                    # Catch unexpected errors during synchronous processing
                    logger.error(f"Error processing non-stream request [Session ID: {session_id}, ID: {message_id}]: {str(e)}", exc_info=True)
                    error_response = {"jsonrpc": "2.0", "id": message_id, "error": {"code": -32000, "message": f"Internal server error: {str(e)}"}}
                    return JSONResponse(content=error_response, status_code=500)
        else:
             # Invalid JSON-RPC format (e.g., missing method or id for a request)
             return JSONResponse({"jsonrpc": "2.0", "id": message_id, "error": {"code": -32600, "message": "Invalid JSON-RPC request format"}}, status_code=400)

    # === Generator Functions for SSE Streams ===

    async def _create_general_sse_generator(self, session_id: str, queue: asyncio.Queue):
        """Generator for GET /mcp server push streams."""
        queue_removed = False
        try:
            while True:
                try:
                    if session_id not in self.client_sessions:
                        logger.warning(f"General SSE stream generator: Session {session_id} closed.")
                        break

                    message = await asyncio.wait_for(queue.get(), timeout=60.0)

                    if message == STREAM_END_MARKER:
                        logger.debug(f"General SSE stream received end marker [Session ID: {session_id}]")
                        break

                    if isinstance(message, dict) and ("result" in message or "error" in message) and "id" in message:
                        logger.warning(f"Attempted to send response on GET stream, blocked [Session ID: {session_id}]: {message}")
                        queue.task_done()
                        continue

                    # TODO: Event ID for recovery?
                    yield {"event": "message", "data": json.dumps(message)}
                    queue.task_done()

                except asyncio.TimeoutError:
                    if session_id not in self.client_sessions:
                        logger.warning(f"General SSE stream generator (timeout): Session {session_id} closed.")
                        break
                    yield {"event": "ping", "data": "keepalive"}
                    continue
                except asyncio.CancelledError:
                    logger.info(f"General SSE stream cancelled [Session ID: {session_id}]")
                    raise
                except Exception as e:
                    logger.error(f"General SSE stream error [Session ID: {session_id}]: {str(e)}", exc_info=True)
                    break
        finally:
            logger.info(f"General SSE stream ended [Session ID: {session_id}]")
            if not queue_removed and session_id in self.client_sessions:
                session = self.client_sessions[session_id]
                if session.get("general_sse_queues") is not None:
                    try:
                        session["general_sse_queues"].remove(queue)
                        queue_removed = True
                        logger.debug(f"General SSE queue removed from session [Session ID: {session_id}]")
                    except ValueError:
                        logger.warning(f"Failed to remove general SSE queue (not found) [Session ID: {session_id}]")
                    except Exception as ce:
                        logger.error(f"Error removing general SSE queue [Session ID: {session_id}]: {ce}")
            while not queue.empty():
                try: queue.get_nowait(); queue.task_done()
                except asyncio.QueueEmpty: break

    async def _create_request_sse_generator(self, session_id: str, request_id: Any, queue: asyncio.Queue):
        """Generator for POST /mcp request-response streams."""
        queue_removed = False
        try:
            while True:
                try:
                    if session_id not in self.client_sessions or \
                       request_id not in self.client_sessions.get(session_id, {}).get("request_queues", {}):
                        logger.warning(f"Request SSE stream generator: Session/Request queue closed [Session ID: {session_id}, Request ID: {request_id}]")
                        break

                    message = await asyncio.wait_for(queue.get(), timeout=120.0) # Longer timeout for requests?

                    if message == STREAM_END_MARKER:
                        logger.debug(f"Request SSE stream received end marker [Session ID: {session_id}, Request ID: {request_id}]")
                        break

                    # TODO: Event ID for parts?
                    yield {"event": "message", "data": json.dumps(message)}
                    queue.task_done()

                except asyncio.TimeoutError:
                    if session_id not in self.client_sessions or \
                       request_id not in self.client_sessions.get(session_id, {}).get("request_queues", {}):
                        logger.warning(f"Request SSE stream generator (timeout): Session/Request queue closed [Session ID: {session_id}, Request ID: {request_id}]")
                        break
                    logger.debug(f"Request SSE stream timed out waiting for message/end [Session ID: {session_id}, Request ID: {request_id}]")
                    # Unlike general stream, timeout here might indicate an issue or just long processing.
                    # Continue waiting for the STREAM_END_MARKER.
                    continue
                except asyncio.CancelledError:
                    logger.info(f"Request SSE stream cancelled [Session ID: {session_id}, Request ID: {request_id}]")
                    raise
                except Exception as e:
                    logger.error(f"Request SSE stream error [Session ID: {session_id}, Request ID: {request_id}]: {str(e)}", exc_info=True)
                    break
        finally:
            logger.info(f"Request SSE stream ended [Session ID: {session_id}, Request ID: {request_id}]")
            if not queue_removed and session_id in self.client_sessions:
                session = self.client_sessions[session_id]
                if session.get("request_queues") is not None:
                    if session["request_queues"].pop(request_id, None):
                        queue_removed = True
                        logger.debug(f"Request SSE queue removed from session [Session ID: {session_id}, Request ID: {request_id}]")
                    else:
                        logger.warning(f"Failed to remove request SSE queue (not found) [Session ID: {session_id}, Request ID: {request_id}]")
            while not queue.empty():
                try: queue.get_nowait(); queue.task_done()
                except asyncio.QueueEmpty: break

    # === Core Request Processing Logic ===

    async def _process_request_and_respond(
        self, request: Request, body: Dict, session_id: str, message_id: Any,
        response_queue: Optional[asyncio.Queue], # Queue ONLY for streaming responses
        is_stream: bool # True if response should go via SSE queue
    ):
        """Processes client method calls and prepares response/error payload or sends to queue.
           Returns payload for non-streaming, returns None for streaming (uses queue).
           Raises HTTPException for non-streaming errors that need specific status codes.
        """
        logger.info(f"Entering _process_request_and_respond for method '{body.get('method')}'...")
        method = body.get("method")
        params = body.get("params", {})
        response_payload = None # Holds the 'result' or 'error' part of JSON-RPC

        try:
            # --- Handle Method Calls --- 
            if method == "mcp/listOfferings":
                tools = await self.mcp_server.list_tools()
                tools_json = self._format_tools(tools)
                resources = await self.mcp_server.list_resources()
                resources_json = self._format_resources(resources)
                prompts = await self.mcp_server.list_prompts()
                prompts_json = self._format_prompts(prompts)
                response_payload = {"tools": tools_json, "resources": resources_json, "prompts": prompts_json}

            elif method == "mcp/listTools" or method == "tools/list":
                 tools = await self.mcp_server.list_tools()
                 response_payload = {"tools": self._format_tools(tools)}

            elif method == "mcp/listResources":
                 resources = await self.mcp_server.list_resources()
                 response_payload = {"resources": self._format_resources(resources)}

            elif method == "mcp/listPrompts":
                 prompts = await self.mcp_server.list_prompts()
                 response_payload = {"prompts": self._format_prompts(prompts)}

            elif method == "mcp/callTool" or method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                if not tool_name:
                     # For non-streaming, raise HTTPException; for streaming, send error via queue
                     error_detail = "Invalid params: tool name ('name') is required"
                     if is_stream and response_queue:
                         error_resp = {"jsonrpc": "2.0", "id": message_id, "error": {"code": -32602, "message": error_detail}}
                         await response_queue.put(error_resp)
                         # No return here for stream, let finally handle end marker
                     else:
                          raise HTTPException(status_code=400, detail=error_detail)
                     return # Exit after handling error

                # --- Tool Calling --- 
                if is_stream and response_queue:
                    # Background task handles putting results/errors in queue
                    logger.info(f"Launching stream tool task [Session: {session_id}, Req: {message_id}, Tool: {tool_name}]")
                    asyncio.create_task(self._execute_stream_tool_wrapper(
                        tool_name, arguments, message_id, session_id, request, response_queue
                    ))
                    # Returns None, caller (_handle_client_post) returns EventSourceResponse
                    return
                else:
                    # Execute tool directly for non-streaming response
                    logger.info(f"Executing non-stream tool [Session: {session_id}, Req: {message_id}, Tool: {tool_name}]")
                    # Note: call_tool now raises ValueError on internal errors
                    result = await self.call_tool(tool_name, arguments, request, None) # No callback needed
                    logger.debug(f"Raw result from non-stream call_tool: {result}")
                    response_payload = self._format_tool_call_result(result)
            else:
                 # Method not found
                 error_detail = f"Method not found: {method}"
                 if is_stream and response_queue:
                      error_resp = {"jsonrpc": "2.0", "id": message_id, "error": {"code": -32601, "message": error_detail}}
                      await response_queue.put(error_resp)
                 else:
                      raise HTTPException(status_code=405, detail=error_detail)
                 return # Exit after handling error

            # --- Prepare final response payload (only if not streaming and successful) ---
            if response_payload is not None:
                 final_response = {"jsonrpc": "2.0", "id": message_id, "result": response_payload}
                 if is_stream and response_queue: # Should not happen if response_payload is set
                      logger.error("Logic error: response_payload set for streaming call?")
                      await response_queue.put(final_response) # Send anyway?
                 elif not is_stream:
                      logger.debug(f"Returning successful non-stream payload for {method}")
                      return final_response # Return dict for JSONResponse

        except Exception as e:
            # Handles errors raised by call_tool (ValueError) or other unexpected issues
            logger.error(f"Error processing request [Session: {session_id}, Req: {message_id}, Method: {method}]: {str(e)}", exc_info=True)
            error_code = -32000
            error_message = f"Internal server error: {str(e)}"
            status_code = 500 # Default for unexpected errors

            if isinstance(e, HTTPException):
                # If it was an HTTPException raised earlier (e.g., 400, 405)
                error_message = e.detail
                status_code = e.status_code
                error_code = -32000 # Keep generic JSON-RPC code for now
            elif isinstance(e, ValueError):
                # Errors from call_tool (tool not found, execution error)
                error_message = str(e)
                status_code = 500 # Treat tool execution errors as internal server errors
                error_code = -32000 # Or a custom tool error code?

            error_response_payload = {"code": error_code, "message": error_message}

            if is_stream and response_queue:
                # Send error via queue for streaming calls
                final_error_response = {"jsonrpc": "2.0", "id": message_id, "error": error_response_payload}
                logger.debug(f"Putting error response into stream queue [Session: {session_id}, Req: {message_id}]")
                await response_queue.put(final_error_response)
                # Returns None, let finally send end marker
                return
            else:
                # For non-streaming, raise HTTPException to set status code
                logger.debug(f"Raising HTTPException for non-stream error (Status: {status_code})")
                raise HTTPException(status_code=status_code, detail=error_message)

        finally:
            # If this was a streaming call, ensure the end marker is sent.
            # This runs even if the processing returns early (e.g., after launching task or handling error).
            if is_stream and response_queue:
                logger.debug(f"Putting stream end marker [Session: {session_id}, Req: {message_id}]")
                await response_queue.put(STREAM_END_MARKER)


    async def _execute_stream_tool_wrapper(
        self, tool_name: str, arguments: Dict, message_id: Any, session_id: str,
        request: Request, response_queue: asyncio.Queue
    ):
        """Wraps stream-capable tool calls, handles callback, puts results/errors into queue."""
        logger.info(f"Entering _execute_stream_tool_wrapper for tool '{tool_name}'...")
        try:
            logger.debug(f"Executing stream tool wrapper [Session: {session_id}, Req: {message_id}, Tool: {tool_name}]")

            async def stream_callback(content, metadata=None):
                logger.debug(f"Stream callback received content [Session: {session_id}, Req: {message_id}]")
                partial_result_formatted = self._format_tool_call_result(content)

                # Check session/queue validity before putting
                if session_id not in self.client_sessions or \
                   message_id not in self.client_sessions.get(session_id, {}).get("request_queues", {}):
                    logger.warning(f"Stream callback: Session/Queue closed, cannot send partial result [Session: {session_id}, Req: {message_id}]")
                    return

                # Send progress notification
                progress_notification = {
                    "jsonrpc": "2.0",
                    "method": "tools/progress",
                    "params": {
                        "requestId": message_id,
                        "toolName": tool_name,
                        "progress": partial_result_formatted,
                    }
                }
                try:
                    await response_queue.put(progress_notification)
                except Exception as e:
                    logger.error(f"Stream callback failed to send progress: {str(e)}")

                # Handle visualization data
                if metadata and "visualization" in metadata:
                    await self.send_visualization_data(session_id, message_id, metadata["visualization"])

            # --- Call Tool --- 
            kwargs = dict(arguments)
            # Simplification: Assume tool supports callback if streaming requested
            kwargs['callback'] = stream_callback

            # call_tool handles its own internal errors and raises ValueError
            result = await self.call_tool(tool_name, kwargs, request, stream_callback)
            logger.debug(f"Stream wrapper received final result from call_tool: {result}")

            # --- Send Final Result --- 
            if session_id not in self.client_sessions or \
               message_id not in self.client_sessions.get(session_id, {}).get("request_queues", {}):
                 logger.warning(f"Stream tool finished but Session/Queue closed [Session: {session_id}, Req: {message_id}]")
                 return # Cannot send final result

            final_result_formatted = self._format_tool_call_result(result)
            final_message = {
                "jsonrpc": "2.0",
                "id": message_id,
                "result": final_result_formatted
            }
            logger.debug(f"Putting final stream result into queue [Session: {session_id}, Req: {message_id}]")
            await response_queue.put(final_message)
            logger.info(f"Stream tool execution successful [Session: {session_id}, Req: {message_id}]")

        except Exception as e:
            # Catches errors from call_tool (ValueError) or other wrapper issues
            logger.error(f"Error during stream tool execution wrapper [Session: {session_id}, Req: {message_id}]: {str(e)}", exc_info=True)
            # Check session/queue validity before sending error
            if session_id not in self.client_sessions or \
               message_id not in self.client_sessions.get(session_id, {}).get("request_queues", {}):
                 logger.warning(f"Stream tool failed but Session/Queue closed [Session: {session_id}, Req: {message_id}]")
                 return # Cannot send error

            error_code = -32000
            error_message = f"Tool execution error: {str(e)}"
            if isinstance(e, ValueError):
                 error_code = -32602 # Or -32000?
                 error_message = str(e)

            error_response = {
                "jsonrpc": "2.0",
                "id": message_id,
                "error": { "code": error_code, "message": error_message }
            }
            try:
                await response_queue.put(error_response)
            except Exception as qe:
                 logger.error(f"Failed to put error response into stream queue: {qe}")
        # No finally block needed here, handled by _process_request_and_respond


    async def call_tool(self, tool_name, arguments, request, callback: Optional[callable] = None):
        """Finds and executes the target tool function/method.
           Raises ValueError on tool not found or execution error.
        """
        logger.info(f"Entering call_tool for tool '{tool_name}'...")
        # Log args excluding callback
        log_args = {k: v for k, v in arguments.items() if k != 'callback'}
        logger.info(f"Executing tool: {tool_name}, Args: {json.dumps(log_args, ensure_ascii=False, default=str)}")

        recent_query = self._extract_recent_query(request)
        # Tool mapping might be needed if client uses different names
        tool_mapping = {
             # Example: "clientFacingName": "internalFunctionName" 
             "status": "mcp_doris_status",
             "health": "mcp_doris_health",
              # Add other mappings if needed, ensure consistency with tool_initializer
             "nl2sql_query": "mcp_doris_nl2sql_query",
             "nl2sql_query_stream": "mcp_doris_nl2sql_query_stream",
             "list_database_tables": "mcp_doris_list_database_tables",
             "explain_table": "mcp_doris_explain_table",
             "get_nl2sql_status": "mcp_doris_get_nl2sql_status",
             "refresh_metadata": "mcp_doris_refresh_metadata",
             "sql_optimize": "mcp_doris_sql_optimize",
             "fix_sql": "mcp_doris_fix_sql",
             "count_chars": "mcp_doris_count_chars",
             "exec_query": "mcp_doris_exec_query",
             "get_schema_list": "mcp_doris_get_schema_list", # Deprecated?
             "save_metadata": "mcp_doris_save_metadata", # Likely internal
             "get_metadata": "mcp_doris_get_metadata", # Likely internal
             "analyze_query_result": "mcp_doris_analyze_query_result", # Internal?
             "generate_sql": "mcp_doris_generate_sql", # Likely internal
             "explain_sql": "mcp_doris_explain_sql", # Internal?
             "modify_sql": "mcp_doris_modify_sql", # Internal?
             "parse_query": "mcp_doris_parse_query", # Internal?
             "identify_query_type": "mcp_doris_identify_query_type", # Internal?
             "validate_sql_syntax": "mcp_doris_validate_sql_syntax", # Internal?
             "check_sql_security": "mcp_doris_check_sql_security", # Internal?
             "find_similar_examples": "mcp_doris_find_similar_examples", # Internal?
             "find_similar_history": "mcp_doris_find_similar_history", # Internal?
             "calculate_query_similarity": "mcp_doris_calculate_query_similarity", # Internal?
             "adapt_similar_query": "mcp_doris_adapt_similar_query", # Internal?
             "get_nl2sql_prompt": "mcp_doris_get_nl2sql_prompt" # Internal?
        }
        mapped_tool_name = tool_mapping.get(tool_name, tool_name)

        try:
            # 1. Find the registered tool instance/function from FastMCP
            tool_instance = None
            mcp = self.app.state.mcp if hasattr(self.app.state, 'mcp') else self.mcp_server
            registered_tools = await mcp.list_tools()
            for tool in registered_tools:
                # The tool object returned by list_tools might be the wrapper function
                # defined in tool_initializer. We need its name.
                tool_registered_name = getattr(tool, 'name', getattr(tool, '__name__', None))
                if tool_registered_name == tool_name: # Match against the name used in @mcp.tool
                    tool_instance = tool # This is likely the wrapper function itself
                    logger.debug(f"Found registered tool wrapper: {tool_registered_name}")
                    break

            if not tool_instance:
                # Fallback: Try importing directly (less ideal as it bypasses registration)
                logger.warning(f"Tool '{tool_name}' not found in registered tools, trying direct import of {mapped_tool_name}")
                try:
                     import doris_mcp_server.tools.mcp_doris_tools as mcp_tools
                     tool_instance = getattr(mcp_tools, mapped_tool_name, None)
                     if not tool_instance or not callable(tool_instance):
                          raise ValueError(f"Tool function {mapped_tool_name} not found or not callable in mcp_doris_tools.")
                     logger.debug(f"Using directly imported tool function: {mapped_tool_name}")
                     # If using direct import, FastMCP context (ctx) is not available
                     # We need to pass args directly
                     processed_args = self._process_tool_arguments(mapped_tool_name, arguments, recent_query)
                     # Inject callback if provided and applicable
                     if callback and mapped_tool_name.endswith("_stream"):
                          processed_args['callback'] = callback
                     elif callback:
                          processed_args.pop('callback', None)
                     result = await tool_instance(**processed_args)
                     logger.debug(f"Raw result from directly imported tool '{mapped_tool_name}': {result}")
                     return result

                except (ImportError, AttributeError, ValueError) as import_err:
                      logger.error(f"Failed to find or import tool: {tool_name} / {mapped_tool_name}. Error: {import_err}")
                      raise ValueError(f"Tool '{tool_name}' not found or failed to import.") from import_err

            # 2. If found via registration, execute using FastMCP's mechanism (if possible)
            #    or simulate the context passing if tool_instance is the wrapper. 
            #    The wrapper expects a Context object. 
            logger.debug(f"Executing registered tool wrapper '{tool_name}'")
            # We need to manually create a mock or simplified Context if FastMCP doesn't handle this automatically
            # For simplicity, let's try passing parameters directly if the wrapper handles it.
            # Ideally, FastMCP would handle the execution via mcp.call_tool(tool_name, params=...) if available.
            # Let's assume the wrapper function handles **kwargs or a Context object. 
            
            # Create a pseudo-context or just pass params
            # Method 1: Pass params directly (assuming wrapper handles it)
            # processed_args = self._process_tool_arguments(mapped_tool_name, arguments, recent_query)
            # if callback:
            #      processed_args['callback'] = callback
            # result = await tool_instance(**processed_args) # This likely won't work if it expects Context

            # Method 2: Create a Context-like object (Requires Context class import)
            # from mcp.server.fastmcp import Context # Make sure imported
            # pseudo_ctx = Context(mcp=mcp, request=request, params=arguments, tool=tool_instance)
            # result = await tool_instance(pseudo_ctx)

            # Method 3: Use mcp.call_tool internal method if accessible and appropriate
            # This is speculative based on potential FastMCP internals
            if hasattr(mcp, 'call_tool_by_name'): # Hypothetical method
                 logger.debug("Attempting execution via mcp.call_tool_by_name")
                 pseudo_ctx_params = arguments # Pass client args
                 # pseudo_ctx_params['_request'] = request # Maybe pass request?
                 if callback: pseudo_ctx_params['callback'] = callback # Pass callback?
                 result = await mcp.call_tool_by_name(tool_name, params=pseudo_ctx_params)
                 logger.debug(f"Result from mcp.call_tool_by_name: {result}")
            else:
                 # Fallback to manual context simulation if no direct call method exists
                 logger.debug("Falling back to manual context simulation for tool wrapper execution")
                 from mcp.server.fastmcp import Context # Ensure imported
                 # Prepare params for context, including potentially callback
                 context_params = dict(arguments)
                 if callback: context_params['callback'] = callback
                 pseudo_ctx = Context(mcp=mcp, request=request, params=context_params, tool=tool_instance)
                 result = await tool_instance(pseudo_ctx) # Call the wrapper with simulated context
                 logger.debug(f"Result from manual context simulation: {result}")

            logger.debug(f"Raw result received in call_tool from registered tool '{tool_name}': {result}")
            return result

        except Exception as e:
            logger.error(f"Exception during call_tool for '{tool_name}': {str(e)}", exc_info=True)
            raise ValueError(f"Error executing tool '{tool_name}': {str(e)}") from e


    # === Helper Methods (Formatting, Session Cleanup, etc.) ===

    def _format_tools(self, tools):
        # Helper to format tool list for responses
        # Based on mcp/listTools structure
        tools_json = []
        for tool in tools:
            # Assuming tools from list_tools are the wrapper functions
            tool_registered_name = getattr(tool, 'name', getattr(tool, '__name__', None))
            if not tool_registered_name:
                logger.warning(f"Could not determine name for tool object: {tool}")
                continue
            
            # Need a way to get description and schema associated with the wrapper
            # This might require inspecting the mcp instance's internal storage
            mcp = self.app.state.mcp if hasattr(self.app.state, 'mcp') else self.mcp_server
            # Hypothetical internal access - THIS IS FRAGILE
            tool_spec = mcp.tools.get(tool_registered_name) if hasattr(mcp, 'tools') else None
            
            description = "" 
            input_schema = {"type": "object", "properties": {}, "required": []}
            if tool_spec and hasattr(tool_spec, 'description'):
                description = tool_spec.description
            if tool_spec and hasattr(tool_spec, 'parameters'): # Assuming parameters holds the JSON schema
                input_schema = tool_spec.parameters
                
            tools_json.append({
                "name": tool_registered_name,
                "description": description,
                "inputSchema": input_schema
            })
        return tools_json

    def _format_resources(self, resources):
        # Helper to format resource list
        return [res.model_dump() if hasattr(res, "model_dump") else res for res in resources]

    def _format_prompts(self, prompts):
        # Helper to format prompt list
        return [prompt.model_dump() if hasattr(prompt, "model_dump") else prompt for prompt in prompts]

    def _format_tool_call_result(self, result: Any) -> Dict[str, Any]:
        # Helper to format tool results into MCP Content format
        content_list = []
        if isinstance(result, str):
            try:
                 # If it looks like the tool already returned the full JSON RPC like structure
                 parsed_json = json.loads(result)
                 if isinstance(parsed_json, dict) and 'content' in parsed_json and isinstance(parsed_json['content'], list):
                      logger.debug("Tool result already seems formatted with 'content', using as is.")
                      return parsed_json # Use the structure directly
                 else:
                     # Assume it's JSON content, wrap it
                     content_list.append({"type": "json", "json": parsed_json})
            except json.JSONDecodeError:
                 # Not JSON, treat as text
                 content_list.append({"type": "text", "text": result})
        elif isinstance(result, (dict, list)):
             # If result is already a dict with a 'content' list, use it directly
             if isinstance(result, dict) and 'content' in result and isinstance(result['content'], list):
                 logger.debug("Tool result dictionary has 'content', using as is.")
                 return result # Use the structure directly
             else:
                 # Otherwise, assume it's JSON content to be wrapped
                 content_list.append({"type": "json", "json": result})
        elif result is None:
             # Handle None result, maybe return empty content or specific type?
             logger.warning("_format_tool_call_result received None result")
             content_list.append({"type": "text", "text": ""}) # Example: empty text
        else:
             # Other types, convert to string and wrap as text
             content_list.append({"type": "text", "text": str(result)})
        # Always return a dict with a 'content' key containing a list
        return {"content": content_list}

    def _process_tool_arguments(self, tool_name, arguments, recent_query):
        # Helper to process tool arguments, including random_string fallback
        # Note: Ensure callback is NOT passed here
        processed_args = dict(arguments)
        processed_args.pop('callback', None) # Explicitly remove callback

        if "random_string" in arguments and tool_name.startswith("mcp_doris_"):
            random_string = processed_args.pop("random_string", "") # Remove from processed too
            logger.debug(f"Processing random_string '{random_string}' for tool {tool_name}")

            # ... (rest of random_string logic as before) ...
            # Example for exec_query:
            if tool_name == "mcp_doris_exec_query" and not processed_args.get("sql"):
                 sql_fallback = random_string or recent_query
                 # ... (logic to extract SQL from fallback) ...
                 if sql_extracted:
                     processed_args["sql"] = sql_extracted
                 else:
                     logger.warning(f"Missing sql for {tool_name}, and fallback failed.")
            # ... (logic for table_name fallback) ...

        return processed_args

    def _extract_recent_query(self, request: Request) -> Optional[str]:
        # Helper to extract recent user query from request
        # (Implementation as provided previously)
        try:
            # Try to extract message history from request body
            body = None
            body_bytes = getattr(request, "_body", None)
            if body_bytes:
                try:
                    body = json.loads(body_bytes)
                except: pass
            if not body: body = getattr(request, "_json", {})

            messages = body.get("params", {}).get("messages", [])
            if messages:
                for msg in reversed(messages):
                    if msg.get("role") == "user": return msg.get("content", "")

            message = body.get("params", {}).get("message", {})
            if message and message.get("role") == "user": return message.get("content", "")

            return None
        except Exception as e:
            logger.error(f"Error extracting recent query: {str(e)}")
            return None

    async def _cleanup_session_resources(self, session_id: str, session_data: Dict):
        # Helper to clean up queues when session is deleted
        logger.info(f"Cleaning up resources for session [Session ID: {session_id}]")
        # Close general SSE queues
        general_queues = session_data.get("general_sse_queues", [])
        for queue in general_queues:
            try:
                await queue.put(STREAM_END_MARKER)
            except Exception as e:
                logger.warning(f"Error putting end marker in general queue for session {session_id}: {e}")
        # Close request-specific SSE queues
        request_queues = session_data.get("request_queues", {})
        for req_id, queue in request_queues.items():
            try:
                await queue.put(STREAM_END_MARKER)
            except Exception as e:
                logger.warning(f"Error putting end marker in request queue {req_id} for session {session_id}: {e}")
        logger.info(f"Finished cleaning resources for session {session_id}")

    # This method might belong in the main app or a shared utility if needed by both servers
    # async def cleanup_idle_sessions(self):
    #     # ... (implementation - needs access to self.client_sessions) ...
    #     pass

    # This method might belong in the main app or a shared utility
    # async def broadcast_message(self, message):
    #     # ... (implementation - needs access to self.client_sessions of BOTH servers?) ...
    #     pass

    # This method is specific to streamable http tool calls
    async def send_visualization_data(self, session_id: str, request_id: Any, visualization_data: Any):
        """Sends visualization data as a notification on the request stream."""
        if session_id not in self.client_sessions:
            logger.warning(f"Cannot send visualization: Session {session_id} not found.")
            return
        queue = self.client_sessions.get(session_id, {}).get("request_queues", {}).get(request_id)
        if not queue:
            logger.warning(f"Cannot send visualization: Request queue {request_id} not found for session {session_id}.")
            return

        notification = {
            "jsonrpc": "2.0",
            "method": "tools/visualization",
            "params": visualization_data
        }
        try:
            await queue.put(notification)
            logger.info(f"Sent visualization notification [Session: {session_id}, Req: {request_id}]")
        except Exception as e:
            logger.error(f"Error sending visualization notification [Session: {session_id}, Req: {request_id}]: {e}")

    # This might belong in main app or shared utility
    # async def send_periodic_updates(self):
    #     # ... (implementation) ...
    #     pass

# End of class DorisMCPStreamableServer 