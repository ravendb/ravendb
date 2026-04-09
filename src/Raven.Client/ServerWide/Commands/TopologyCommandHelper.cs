using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.Util;
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
                    catch (Exception e)
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

            return Encoding.UTF8.GetString(buffer.Address, buffer.Valid);
        }

    }
}
