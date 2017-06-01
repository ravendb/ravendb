using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using Raven.Client.Http;

namespace Raven.Server.Exceptions.PeriodicBackup
{
    public class StorageException : Exception
    {
        public HttpResponseMessage Response { get; }

        public HttpStatusCode StatusCode => Response.StatusCode;

        public string ResponseString { get; private set; }

        private StorageException(HttpResponseMessage response, string message)
            : base(message)
        {
            Response = response;
        }

        public static StorageException FromResponseMessage(HttpResponseMessage response)
        {
            var sb = new StringBuilder("Status code: ").Append(response.StatusCode).AppendLine();

            string responseString = null;
            if (response.Content != null)
            {
                var readAsStringAsync = RequestExecutor.ReadAsStreamUncompressedAsync(response);
                if (readAsStringAsync.IsCompleted)
                {
                    using(var stream = readAsStringAsync.Result)
                    using (var streamReader = new StreamReader(stream))
                    {
                        responseString = streamReader.ReadToEnd();
                        sb.AppendLine(responseString);
                    }
                }
            }

            return new StorageException(response, sb.ToString())
            {
                ResponseString = responseString
            };
        }
    }
}