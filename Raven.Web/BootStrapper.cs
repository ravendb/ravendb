using System.Web;
using Microsoft.Web.Infrastructure.DynamicModuleHelper;
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
			ForwardToRavenRespondersFactory.Shutdown();
		}
	}

	public static class BootStrapper
	{
		public static void Init()
		{
			DynamicModuleUtility.RegisterModule(typeof(RavenDbStartupAndShutdownModule));
		}
	}
}