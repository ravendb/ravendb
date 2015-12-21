using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Connection
{
    [Serializable]
    public class ErrorResponseException : Exception
    {
#if !DNXCORE50
        [NonSerialized]
#endif
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

        public ErrorResponseException(HttpResponseMessage response, string msg, string responseString = null)
            : base(msg)
        {
            this.response = response;
            ResponseString = responseString;
        }

        public static ErrorResponseException FromHttpRequestException(HttpRequestException exception)
        {
            var ex = new ErrorResponseException(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable), exception.Message, exception.Message);

            foreach (var key in exception.Data.Keys)
                ex.Data[key] = exception.Data[key];

            return ex;
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
            return new ErrorResponseException(response, sb.ToString())
            {
                ResponseString = responseString
            };
        }

        public string ResponseString { get; private set; }

        public Etag Etag
        {
            get
            {
                if (Response.Headers.ETag == null)
                    return null;
                var responseHeader = Response.Headers.ETag.Tag;

                if (responseHeader[0] == '\"')
                    return Etag.Parse(responseHeader.Substring(1, responseHeader.Length - 2));

                return Etag.Parse(responseHeader);
            }
        }

#if !DNXCORE50
        protected ErrorResponseException(
            System.Runtime.Serialization.SerializationInfo info,
            System.Runtime.Serialization.StreamingContext context)
            : base(info, context)
        {
        }
#endif
    }
}
