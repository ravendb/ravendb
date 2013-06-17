using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;

namespace Raven.Client.Connection
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

#if !NETFX_CORE
		protected ErrorResponseException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
#endif
	}
}