using System;

namespace Raven.Server.Web.System
{
    public static class UrlHelper
    {
        public static string TryGetLeftPart(string originalUrl)
        {
            originalUrl = originalUrl.TrimEnd('/');

            if (originalUrl.IndexOf("/studio/index.html", StringComparison.OrdinalIgnoreCase) > -1 || //3.x, 4.x
                originalUrl.IndexOf("/raven/studio.html", StringComparison.OrdinalIgnoreCase) > -1) //2.x 
            {
                var uri = new Uri(originalUrl);
                return uri.GetLeftPart(UriPartial.Authority);
            }

            return originalUrl;
        }
    }
}
