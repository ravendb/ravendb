using System.ComponentModel.Composition;
using System.Net;

namespace Raven.Database.Server
{
	[InheritedExport]
	public interface IConfigureHttpListener
	{
		void Configure(HttpListener listener, IRavenHttpConfiguration config);
	}
}