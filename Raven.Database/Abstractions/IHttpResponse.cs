using System.Collections.Specialized;
using System.IO;

namespace Raven.Database.Abstractions
{
	public interface IHttpResponse
	{
		NameValueCollection Headers { get; }
		Stream OutputStream { get; }
		long ContentLength64 { get; set; }
		int StatusCode { get; set; }
		string StatusDescription { get; set; }
		void Redirect(string url);
		void Close();
	}
}