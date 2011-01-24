namespace Raven.Management.Client.Silverlight.Common.Converters
{
    using System.Net;
    using Abstractions.Data;

    public static class WebHeaderCollectionConverter
    {
        public static NameValueCollection ConvertToNameValueCollection(this WebHeaderCollection headers)
        {
            var result = new NameValueCollection();
            foreach (string key in headers.AllKeys)
            {
                result.Add(key, headers[key]);
            }

            return result;
        }
    }
}