#if NETSTANDARD2_0

using System.Buffers;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace System.Net.WebSockets
{
    internal static class WebSocketExtensions
    {
        /// <summary>
        /// https://github.com/dotnet/runtime/blob/0f829188c6c1ca35a951214f8e9c43f377953b96/src/libraries/System.Net.WebSockets/src/System/Net/WebSockets/WebSocket.cs#L36-L55
        /// </summary>
        public static async ValueTask<WebSocketReceiveResult> ReceiveAsync(this WebSocket webSocket, Memory<byte> buffer, CancellationToken cancellationToken)
        {
            if (MemoryMarshal.TryGetArray(buffer, out ArraySegment<byte> arraySegment))
            {
                return await webSocket.ReceiveAsync(arraySegment, cancellationToken).ConfigureAwait(false);
            }

            byte[] array = ArrayPool<byte>.Shared.Rent(buffer.Length);
            try
            {
                WebSocketReceiveResult r = await webSocket.ReceiveAsync(new ArraySegment<byte>(array, 0, buffer.Length), cancellationToken).ConfigureAwait(false);
                new Span<byte>(array, 0, r.Count).CopyTo(buffer.Span);
                return r;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(array);
            }
        }
    }
}

#endif
