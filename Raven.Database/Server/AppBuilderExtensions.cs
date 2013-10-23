using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.Dispatcher;
using System.Web.Http.Hosting;
using Microsoft.Owin;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Server.Connections;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi.Filters;
using Raven.Database.Server.WebApi.Handlers;

// ReSharper disable once CheckNamespace
namespace Owin
{
	public static class AppBuilderExtensions
	{
		public static IAppBuilder UseRavenDB(this IAppBuilder app)
		{
			return UseRavenDB(app, new RavenConfiguration());
		}

		public static IAppBuilder UseRavenDB(this IAppBuilder app, InMemoryRavenConfiguration configuration)
		{
			return UseRavenDB(app, new RavenDBOptions(configuration));
		}

		private static IAppBuilder UseInterceptor(this IAppBuilder app, DatabasesLandlord databasesLandlord)
		{
			return app.Use(typeof (InterceptMiddleware), new[] {databasesLandlord});
		}

		public static IAppBuilder UseRavenDB(this IAppBuilder app, RavenDBOptions options)
		{
			if (options == null)
			{
				throw new ArgumentNullException("options");
			}

			// This is a katana specific key (i.e. non a standard OWIN key) to be notified
			// when the host in being shut down. Works both in HttpListener and SystemWeb hosting
			var appDisposing = app.Properties["host.OnAppDisposing"] as CancellationToken?;
			if (appDisposing.HasValue)
			{
				appDisposing.Value.Register(options.Dispose);
			}

			var httpConfiguration = new HttpConfiguration();
			SetupConfig(httpConfiguration, options.Landlord, options.MixedModeRequestAuthorizer);
			app.UseInterceptor(options.Landlord)
				.UseWebApi(httpConfiguration);
			return app;
		}

		private static void SetupConfig(HttpConfiguration cfg, DatabasesLandlord databasesLandlord,
			MixedModeRequestAuthorizer mixedModeRequestAuthorizer)
		{
			cfg.Properties[typeof(DatabasesLandlord)] = databasesLandlord;
			cfg.Properties[typeof(MixedModeRequestAuthorizer)] = mixedModeRequestAuthorizer;
			cfg.Formatters.Remove(cfg.Formatters.XmlFormatter);

			cfg.Services.Replace(typeof(IAssembliesResolver), new MyAssemblyResolver());
			cfg.Filters.Add(new RavenExceptionFilterAttribute());
			cfg.MapHttpAttributeRoutes();
			cfg.Routes.MapHttpRoute(
				"API Default", "{controller}/{action}",
				new { id = RouteParameter.Optional });

			cfg.Routes.MapHttpRoute(
				"Database Route", "databases/{databaseName}/{controller}/{action}",
				new { id = RouteParameter.Optional });
			cfg.MessageHandlers.Add(new GZipToJsonHandler());
			cfg.Services.Replace(typeof(IHostBufferPolicySelector), new SelectiveBufferPolicySelector());
		}

		private class MyAssemblyResolver : IAssembliesResolver
		{
			public ICollection<Assembly> GetAssemblies()
			{
				return new[] { typeof(RavenApiController).Assembly };
			}
		}

		public class SelectiveBufferPolicySelector : IHostBufferPolicySelector
		{
			public bool UseBufferedInputStream(object hostContext)
			{
				var context = hostContext as IOwinContext;

				if (context != null)
				{
					if (context.Request.Uri.LocalPath.EndsWith("bulkInsert", StringComparison.OrdinalIgnoreCase))
						return false;
				}

				return true;
			}

			public bool UseBufferedOutputStream(HttpResponseMessage response)
			{
				return (response.Content is ChangesPushContent ||
						response.Content is StreamsController.StreamQueryContent ||
						response.Content is StreamContent ||
						response.Content is PushStreamContent ||
						response.Content is JsonContent ||
						response.Content is MultiGetController.MultiGetContent) == false;
			}
		}

		private class InterceptMiddleware : OwinMiddleware
		{
			private readonly DatabasesLandlord databasesLandlord;

			public InterceptMiddleware(OwinMiddleware next, DatabasesLandlord databasesLandlord) : base(next)
			{
				this.databasesLandlord = databasesLandlord;
			}

			public override async Task Invoke(IOwinContext context)
			{
				// Prerequest stuff

				await Next.Invoke(context);
				// Post request stuff
			}
		}
	}
}