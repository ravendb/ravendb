using System.ComponentModel.Composition;
using System.Net;

namespace Raven.Http
{
	[InheritedExport]
	public interface IConfigureHttpListener
	{
		void Configure(HttpListener listener, IRavenHttpConfiguration config);
	}
}