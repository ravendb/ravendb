using System.ComponentModel.Composition;
using System.Net;
using Raven.Database.Config;

namespace Raven.Database.Server
{
	[InheritedExport]
	public interface IConfigureHttpListener
	{
		void Configure(HttpListener listener, InMemoryRavenConfiguration config);
	}
}