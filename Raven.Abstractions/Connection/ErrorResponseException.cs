using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Connection
{
#if !NETFX_CORE
    [Serializable]
#endif
    public class ErrorResponseException : Exception
    {
	    public HttpResponseMessage Response { get; private set; }

        public HttpStatusCode StatusCode
        {
            get { return Response.StatusCode; }
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

#if !NETFX_CORE
        protected ErrorResponseException(
            System.Runtime.Serialization.SerializationInfo info,
            System.Runtime.Serialization.StreamingContext context)
            : base(info, context)
        {
        }
#endif
    }
}