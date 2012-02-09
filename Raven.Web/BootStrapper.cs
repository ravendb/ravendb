using System.Web;
using Microsoft.Web.Infrastructure.DynamicModuleHelper;
using Raven.Database.Server;
using Raven.Web;

[assembly: PreApplicationStartMethod(typeof(BootStrapper), "Init")]
namespace Raven.Web
{
	public class RavenDbStartupAndShutdownModule : IHttpModule
	{
		public void Init(HttpApplication context)
		{
			context.BeginRequest += (sender, args) => ForwardToRavenRespondersFactory.Init();
		}

		public void Dispose()
		{
		}
	}

	public static class BootStrapper
	{
		public static void Init()
		{
			HttpEndpointRegistration.RegisterHttpEndpointTarget();
			DynamicModuleUtility.RegisterModule(typeof(RavenDbStartupAndShutdownModule));
		}
	}
}