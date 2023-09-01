namespace Raven.Server.Utils
{
    public static class UrlUtil
    {
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
