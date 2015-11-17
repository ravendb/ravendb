using System;
using System.ComponentModel;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
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

        public static ErrorResponseException FromException(HttpRequestException e)
        {
            var builder = new StringBuilder();

            var webException = e.InnerException as WebException;
            var statusCode = HttpStatusCode.ServiceUnavailable;
            if (webException != null)
            {
                builder.Append("WebExceptionMessage: ");
                builder.AppendLine(webException.Message);
                builder.Append("Status Code: ");
                builder.AppendLine(webException.Status.ToString());
                var webResponse = webException.Response as HttpWebResponse;
                if (webResponse != null)
                {
                    builder.Append("Response Status Code: ");
                    statusCode = webResponse.StatusCode;
                    builder.AppendLine(statusCode.ToString());

                    try
                    {
                    using (var stream = webResponse.GetResponseStreamWithHttpDecompression())
                    using (var reader = new StreamReader(stream))
                    {
                        builder.Append("Response: ");
                        builder.AppendLine(reader.ReadToEnd());
                        }
                    }
                    catch (Exception e2)
                    {
                        builder.Append("Failed to read the response: " + e2);
                    }

                }

                var win32Exception = webException.InnerException as Win32Exception;
                if (win32Exception != null)
                {
                    builder.Append("Win32 Error: ");
                    builder.AppendLine(win32Exception.Message);
                }
            }

            return new ErrorResponseException
            {
                Response = new HttpResponseMessage(statusCode),
                ResponseString = builder.ToString(),
            };
        }
    }
}
#endif
