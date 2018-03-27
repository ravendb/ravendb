using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Server.Smuggler.Migration.ApiKey
{
    [Serializable]
    public class ErrorResponseException : Exception
    {
        private readonly HttpResponseMessage response;

        public HttpResponseMessage Response
        {
            get { return response; }
        }

        public HttpStatusCode StatusCode
        {
            get { return Response.StatusCode; }
        }

        public ErrorResponseException()
        {
        }

        public ErrorResponseException(ErrorResponseException e, string message)
            : base(message)
        {
            response = e.Response;
            ResponseString = e.ResponseString;
        }

        public ErrorResponseException(HttpResponseMessage response, string msg, Exception exception)
            : base(msg, exception)
        {
            this.response = response;
        }

        public ErrorResponseException(HttpResponseMessage response, string msg, string responseString = null, Exception inner = null)
            : base(msg, inner)
        {
            this.response = response;
            ResponseString = responseString;
        }

        public static ErrorResponseException FromResponseMessage(HttpResponseMessage response, bool readErrorString = true)
        {
            var sb = new StringBuilder("Status code: ").Append(response.StatusCode).AppendLine();

            string responseString = null;
            if (readErrorString && response.Content != null)
            {
                var readAsStringAsync = response.GetResponseStreamWithHttpDecompression();
                if (readAsStringAsync.IsCompleted)
                {
                    using (var streamReader = new StreamReader(readAsStringAsync.Result))
                    {
                        responseString = streamReader.ReadToEnd();
                        sb.AppendLine(responseString);
                    }
                }
            }
            return new ErrorResponseException(response, sb.ToString(), null, null)
            {
                ResponseString = responseString
            };
        }

        public string ResponseString { get; private set; }

        public static ErrorResponseException FromException(TaskCanceledException e)
        {
            return new ErrorResponseException(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable), e.Message, "Unable to connect to the remote server\r\nStatus Code: ConnectFailure", e);
        }

        public static ErrorResponseException FromException(HttpRequestException e)
        {
            var builder = new StringBuilder();
            var statusCode = HttpStatusCode.ServiceUnavailable;

            return new ErrorResponseException(new HttpResponseMessage(statusCode), e.Message, builder.ToString(), e);
        }
    }
}