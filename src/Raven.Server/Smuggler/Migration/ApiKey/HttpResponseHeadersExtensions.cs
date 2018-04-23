using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Headers;

namespace Raven.Server.Smuggler.Migration.ApiKey
{
    internal static class HttpResponseHeadersExtensions
    {
        /// <returns>
        /// Returns <see cref="T:System.Collections.Generic.IEnumerable`1"/>.
        /// </returns>
        public static string GetFirstValue(this HttpHeaders headers, string name)
        {
            IEnumerable<string> values;
            if (headers.TryGetValues(name, out values) == false)
                return null;

            return values.FirstOrDefault();
        }
    }
}
