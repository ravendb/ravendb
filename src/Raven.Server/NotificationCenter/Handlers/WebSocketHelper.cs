using System;
using System.Net.WebSockets;
using Raven.Client.Extensions;

namespace Raven.Server.NotificationCenter.Handlers
{
    internal static class WebSocketHelper
    {
        internal static bool IsSocketClosed(Exception ex, params WebSocket[] sockets)
        {
            if (ex is AggregateException ae)
                ex = ae.ExtractSingleInnerException();

            if (ex is not WebSocketException webSocketException)
                return false;

            if (webSocketException.WebSocketErrorCode == WebSocketError.ConnectionClosedPrematurely)
                return true;

            if (webSocketException.WebSocketErrorCode != WebSocketError.InvalidState)
                return false;

            // if we received close from the client, we want to ignore it and close the websocket (dispose does it)
            foreach (WebSocket socket in sockets)
            {
                WebSocketState state = socket.State;
                if (state == WebSocketState.Closed || state == WebSocketState.CloseReceived || state == WebSocketState.Aborted)
                    return true;
            }

            return false;
        }
    }
}
