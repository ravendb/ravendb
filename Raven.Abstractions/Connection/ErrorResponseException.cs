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

		public ErrorResponseException(string message) : base(message)
		{
		}

		public ErrorResponseException(string message, Exception inner) : base(message, inner)
		{
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