using System;
using System.Net.Http;
using System.Net.Http.Headers;

namespace Raven.Database.FileSystem.Extensions
{
    public static class HttpExtensions
    {
        public static HttpResponseMessage WithNoCache(this HttpResponseMessage message)
        {
            // Ensure that files are not cached at the browser side.
            // "Cache-Control": "no-cache, no-store, must-revalidate";
            // "Expires": 0;
            message.Headers.CacheControl = new CacheControlHeaderValue()
            {
                MustRevalidate = true,
                NoCache = true,
                NoStore = true,
                MaxAge = new TimeSpan(0, 0, 0, 0)
            };

            return message;
        }
    }
}
