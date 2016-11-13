using System;
using System.Collections.Generic;
using Raven.NewClient.Json.Linq;

namespace Raven.Abstractions.Data
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
        public RavenJToken Result { get; set; }

        /// <summary>
        /// Response headers.
        /// </summary>
        public IDictionary<string,string> Headers { get; set; }

        /// <summary>
        /// Response HTTP status code.
        /// </summary>
        public int Status { get; set; }

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
            switch (Status)
            {
                case 0:   // aggressively cached
                case 200: // known non error values
                case 201:
                case 203:
                case 204:
                case 304:
                case 404:
                    return false;
                default:
                    return true;
            }
        }
    }
}
