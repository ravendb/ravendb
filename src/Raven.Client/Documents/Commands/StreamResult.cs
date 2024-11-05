using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Client.Util;

namespace Raven.Client.Documents.Commands
{
    public class StreamResult : IDisposable, IAsyncDisposable
    {
        public StreamResult(Stream stream, HttpResponseMessage response)
        {
            Response = response;
            Stream = new StreamWithTimeout(stream);
        }
        public HttpResponseMessage Response { get; }
        public Stream Stream { get; }

        public void Dispose()
        {
            using (Response)
            using (Stream)
            {
            }
        }

#if NETSTANDARD2_0
        public ValueTask DisposeAsync()
        {
            using (this)
            {

            }

            return default;
        }
#else
        public async ValueTask DisposeAsync()
        {
            using (Response)
            {
                await Stream.DisposeAsync().ConfigureAwait(false);
            }
        }
#endif

    }

    public sealed class StreamResult<TType> : AbstractStreamResult
    {
        /// <summary>
        /// Document deserialized to <c>TType</c>.
        /// </summary>
        public TType Document { get; set; }
    }

    public sealed class TimeSeriesStreamResult<TType> : AbstractStreamResult
    {
        /// <summary>
        /// Time Series deserialized to <c>TType</c>.
        /// </summary>
        public TType Result { get; set; }
    }

    public abstract class AbstractStreamResult
    {
        /// <summary>
        /// Document ID.
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Document change vector.
        /// </summary>
        public string ChangeVector { get; set; }

        /// <summary>
        /// Document metadata.
        /// </summary>
        public IMetadataDictionary Metadata { get; set; }
    }
}
