using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
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
			:base(GenerateMessage(response))
        {
            Response = response;
        }

		private static string GenerateMessage(HttpResponseMessage response)
		{
			var sb = new StringBuilder("Status code: ").Append(response.StatusCode).AppendLine();

			if (response.Content != null)
			{
                var readAsStringAsync = response.GetResponseStreamWithHttpDecompression();
			    if (readAsStringAsync.IsCompleted)
			    {
			        using (var streamReader = new StreamReader(readAsStringAsync.Result))
			            sb.AppendLine(streamReader.ReadToEnd());
			    }
			}
			return sb.ToString();
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