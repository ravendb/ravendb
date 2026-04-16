using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Exceptions;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands
{
    internal static class TopologyCommandHelper
    {
        private const int MaxTopologyResponseSize = 1024 * 1024; // 1 MB is crazy high limit for topology response

        public static async Task<BlittableJsonReaderObject> ParseTopologyResponseAsync(
            JsonOperationContext context, HttpResponseMessage response, string url, string debugTag)
        {
#if NETSTANDARD2_0
            using (var responseStream = await response.Content.ReadAsStreamWithZstdSupportAsync().ConfigureAwait(false))
#else
            await using (var responseStream = await response.Content.ReadAsStreamWithZstdSupportAsync().ConfigureAwait(false))
#endif
            await using (var stream = new StreamWithTimeout(responseStream))
            {
                using (context.GetMemoryBuffer(out var buffer))
                {
                    try
                    {
                        return await context.ParseToMemoryAsync(stream, debugTag,
                            BlittableJsonDocumentBuilder.UsageMode.None, buffer, maxSize: MaxTopologyResponseSize).ConfigureAwait(false);
                    }
                    catch (Exception e) when (e.InnerException is null && // only catch exceptions that are directly thrown by the parsing code
                                              e is InvalidDataException   // and only those that are raised directly by the parsing code, not by the HTTP layer or other code
                                                or InvalidStartOfObjectException 
                                                or FormatException 
                                                or EndOfStreamException 
                                                or ArgumentException 
                                                or InvalidOperationException)
                    {
                        string body = GetLastReadBytes(buffer);

                        throw new InvalidOperationException(
                            $"Failed to parse the topology response from '{url}'. " +
                            $"This may indicate that the URL does not point to a RavenDB server. Response: {body}", e);
                    }
                }
            }
        }

        public static void ThrowUnexpectedTopologyResponse(string url, BlittableJsonReaderObject json)
        {
            throw new InvalidOperationException(
                $"Received an unexpected topology response from '{url}'. " +
                $"This may indicate that the URL does not point to a RavenDB server. Response: {json}");
        }

        private static unsafe string GetLastReadBytes(JsonOperationContext.MemoryBuffer buffer)
        {
            if (buffer.Valid == 0)
                return string.Empty;

            int size = Math.Min(buffer.Valid, PeepingTomStream.BufferWindowSize);
            var text = Encoding.UTF8.GetString(buffer.Address, size);
            if (buffer.Valid > PeepingTomStream.BufferWindowSize)
                text += "... (truncated)";
            return text;
        }

    }
}
