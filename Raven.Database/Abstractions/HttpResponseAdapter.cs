using System.Collections.Specialized;
using System.IO;
using System.Web;

namespace Raven.Server.Abstractions
{
	public class HttpResponseAdapter : IHttpResponse
	{
		private readonly HttpResponse response;

		public HttpResponseAdapter(HttpResponse response)
		{
			this.response = response;
		}

		public NameValueCollection Headers
		{
			get { return response.Headers; }
		}

		public Stream OutputStream
		{
			get { return response.OutputStream; }
		}

		public long ContentLength64
		{
			get { return -1; }
			set { }
		}

		public int StatusCode
		{
			get { return response.StatusCode; }
			set { response.StatusCode = value; }
		}

		public string StatusDescription
		{
			get { return response.StatusDescription; }
			set { response.StatusDescription = value; }
		}

		public void Redirect(string url)
		{
			response.Redirect(url);
		}

		public void Close()
		{
			response.Close();
		}
	}
}