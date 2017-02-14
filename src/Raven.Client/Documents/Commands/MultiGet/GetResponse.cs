using System;
using System.Collections.Generic;
using System.Net;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands.MultiGet
{
    public class GetResponse
    {
        public GetResponse()
        {
            Headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Response result as JSON.
        /// </summary>
        public BlittableJsonReaderBase Result { get; set; }

        /// <summary>
        /// Response headers.
        /// </summary>
        public Dictionary<string, string> Headers { get; set; }

        /// <summary>
        /// Response HTTP status code.
        /// </summary>
        public HttpStatusCode StatusCode { get; set; }

        /// <summary>
        /// Indicates if request should be retried (forced).
        /// </summary>
        public bool ForceRetry { get; set; }

        /// <summary>
        /// Method used to check if request has errors.
        /// <para>Returns:</para>
        /// <para>- <c>false</c> - if Status is 0, 200, 201, 203, 204, 304 and 404</para>
        /// <para>- <c>true</c> - otherwise</para>
        /// </summary>
        /// <returns><c>false</c> if Status is 0, 200, 201, 203, 204, 304 and 404. <c>True</c> otherwise.</returns>
        public bool RequestHasErrors()
        {
            switch (StatusCode)
            {
                case 0:                     // aggressively cached
                case HttpStatusCode.OK:     // known non error values
                case HttpStatusCode.Created:
                case HttpStatusCode.NonAuthoritativeInformation:
                case HttpStatusCode.NoContent:
                case HttpStatusCode.NotModified:
                case HttpStatusCode.NotFound:
                    return false;
                default:
                    return true;
            }
        }
    }
}
