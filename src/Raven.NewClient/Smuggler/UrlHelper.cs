using System.Collections.Generic;
using System.Linq;

namespace Raven.NewClient.Client.Smuggler
{
    public static class UrlHelper
    {
        public static string BuildUrl(string url, Dictionary<string, object> query)
        {
            if (query.Count > 0)
                url += "?" + string.Join("&", query.Select(pair => pair.Key + "=" + pair.Value.ToString()));
            return url;
        }
    }
}