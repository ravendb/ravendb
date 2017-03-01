using System.Collections.Generic;
using System.IO;
using System.Net.Http;

namespace Raven.Client.Documents.Commands
{
    public class StreamResult
    {
        public HttpResponseMessage Response { get; set; }
        public Stream Stream { get; set; }
    }

    public class StreamResult<TType>
    {
        /// <summary>
        /// Document key.
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// Document etag.
        /// </summary>
        public long Etag { get; set; }

        /// <summary>
        /// Document metadata.
        /// </summary>
        public IDictionary<string, object> Metadata { get; set; }

        /// <summary>
        /// Document deserialized to <c>TType</c>.
        /// </summary>
        public TType Document { get; set; }
    }
}