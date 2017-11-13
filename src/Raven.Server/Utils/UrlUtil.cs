using System;

namespace Raven.Server.Utils
{
    public static class UrlUtil
    {
        public static string EnsureValidExternalUrl(string url)
        {
            if (Uri.TryCreate(url, UriKind.Absolute, out var uri))
            {
                if (IsZeros(uri.Host))
                {
                    url = new UriBuilder(uri)
                    {
                        Host = Environment.MachineName
                    }.Uri.ToString();
                }
            }
            return url.TrimEnd('/');
        }

        public static string TrimTrailingSlash(string url)
        {
            return url.TrimEnd('/');
        }

        public static bool IsZeros(string hostName)
        {
            switch (hostName)
            {
                case "::":
                case "::0":
                case "0.0.0.0":
                    return true;
            }

            return false;
        }
    }
}
