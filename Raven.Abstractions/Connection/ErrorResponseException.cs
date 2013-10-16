using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace Raven.Abstractions.Connection
{
#if !NETFX_CORE && !SILVERLIGHT
	[Serializable]
#endif
	public class ErrorResponseException : Exception
	{
		public HttpResponseMessage Response { get; private set; }

		public HttpStatusCode StatusCode
		{
			get { return Response.StatusCode; }
		}

		public async Task<string> GetMessage()
		{
			using (var sr = new StreamReader(await Response.GetResponseStreamWithHttpDecompression()))
			{
				return sr.ReadToEnd();
			}
		}

		public ErrorResponseException()
		{
		}

		public ErrorResponseException(HttpResponseMessage response, string msg, Exception exception)
             : base(msg, exception)
		{
			Response = response;
		}

        public ErrorResponseException(HttpResponseMessage response, string msg)
            : base(msg)
        {
            Response = response;
        }

		public ErrorResponseException(HttpResponseMessage response)
        {
            Response = response;
        }

#if !NETFX_CORE && !SILVERLIGHT
		protected ErrorResponseException(
			System.Runtime.Serialization.SerializationInfo info,
			System.Runtime.Serialization.StreamingContext context) : base(info, context)
		{
		}
#endif
	}
}