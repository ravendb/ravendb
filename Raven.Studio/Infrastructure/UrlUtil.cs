using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
    public static class UrlUtil
    {
        public static int GetSkipCount()
        {
            var queryParam = ApplicationModel.Current.GetQueryParam("skip");
            if (string.IsNullOrEmpty(queryParam))
                return 0;
            int result;
            int.TryParse(queryParam, out result);
            return result;
        }
    }
}