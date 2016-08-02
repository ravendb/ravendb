using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Http;

namespace Raven.Abstractions.Connection
{
    public class ErrorResponseException : Exception
    {
        public HttpResponseMessage Response { get; private set; }

        public HttpStatusCode StatusCode
        {
            get { return Response.StatusCode; }
        }

        public ErrorResponseException()
        {				
        }

        public ErrorResponseException(ErrorResponseException e, string message)
            :base(message)
        {
            Response = e.Response;
            ResponseString = e.ResponseString;
        }

        public ErrorResponseException(HttpResponseMessage response, string msg, Exception exception)
            : base(msg, exception)
        {
            Response = response;
        }

        public ErrorResponseException(HttpResponseMessage response, string msg, string responseString= null)
            : base(msg)
        {
            Response = response;
            ResponseString = responseString;
        }

        public static ErrorResponseException FromHttpRequestException(HttpRequestException exception)
        {
            var ex = new ErrorResponseException(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable), exception.Message, exception.InnerException)
            {
                ResponseString = exception.Message,
            };

            foreach (var key in exception.Data.Keys)
                ex.Data[key] = exception.Data[key];

            return ex;
        }

        public static ErrorResponseException FromResponseMessage(HttpResponseMessage response, bool readErrorString = true)
        {
            var sb = new StringBuilder("Status code: ").Append(response.StatusCode).AppendLine();

            string responseString = null;
            if (readErrorString)
            {
                responseString = response.ReadErrorResponse();
                sb.AppendLine(responseString);
            }

            return new ErrorResponseException(response, sb.ToString())
            {
                ResponseString = responseString
            };
        }

        public string ResponseString { get; private set; }

        public long? Etag
        {
            get
            {
                if (Response.Headers.ETag == null)
                    return null;
                var responseHeader = Response.Headers.ETag.Tag;

                if (responseHeader[0] == '\"')
                    return long.Parse(responseHeader.Substring(1, responseHeader.Length - 2));

                return long.Parse(responseHeader);
            }
        }
    }
}
