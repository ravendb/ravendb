using System.IO;
using System.Net.Http;
using Raven.Abstractions.Connection;

namespace Raven.Client.Http
{
    public static class HttpResponseMessageExtensions
    {
        public static string ReadErrorResponse(this HttpResponseMessage response)
        {
            if (response.Content == null)
                return null;
            
            var readAsStringAsync = response.GetResponseStreamWithHttpDecompression();
            if (readAsStringAsync.IsCompleted)
            {
                using (var streamReader = new StreamReader(readAsStringAsync.Result))
                {
                    return streamReader.ReadToEnd();
                }
            }

            // TODO: Log this?
            return null;
        }
    }
}