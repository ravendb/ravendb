using System;

namespace Raven.Server.Utils
{
    public sealed class WebSocketHelper
    {
        public static readonly ArraySegment<byte> Heartbeat = new ArraySegment<byte>(new[] { (byte)'\r', (byte)'\n' });
    }
}