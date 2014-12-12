using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.Controllers;
using System.Web.Http.Dispatcher;
using System.Web.Http.Hosting;
using System.Web.Http.Routing;
using System.Web.UI;
using Microsoft.Owin;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Server.Connections;
using Raven.Database.Server.Controllers;
using Raven.Database.FileSystem.Util;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi;
using Raven.Database.Server.WebApi.Filters;
using Raven.Database.Server.WebApi.Handlers;
using System.Net;

// ReSharper disable once CheckNamespace
namespace Owin
{
	public static class AppBuilderExtensions
	{
		private const string HostOnAppDisposing = "host.OnAppDisposing";

		public static IAppBuilder UseRavenDB(this IAppBuilder app)
		{
			return UseRavenDB(app, new RavenConfiguration());
		}

		public static IAppBuilder UseRavenDB(this IAppBuilder app, InMemoryRavenConfiguration configuration)
		{
			return UseRavenDB(app, new RavenDBOptions(configuration));
		}

		private static IAppBuilder UseInterceptor(this IAppBuilder app)
		{
			return app.Use(typeof(InterceptMiddleware));
		}


		public static IAppBuilder UseRavenDB(this IAppBuilder app, RavenDBOptions options)
		{
			if (options == null)
			{
				throw new ArgumentNullException("options");
			}

			if (app.Properties.ContainsKey(HostOnAppDisposing))
			{
				// This is a katana specific key (i.e. not a standard OWIN key) to be notified
				// when the host in being shut down. Works both in HttpListener and SystemWeb hosting
				// Until owin spec is officially updated, there is no other way to know the host
				// is shutting down / disposing
				var appDisposing = app.Properties[HostOnAppDisposing] as CancellationToken?;
				if (appDisposing.HasValue)
				{
					appDisposing.Value.Register(options.Dispose);
				}
			}

			AssemblyExtractor.ExtractEmbeddedAssemblies();

#if DEBUG
			app.UseInterceptor();
#endif

			app.Use((context, func) => UpgradeToWebSockets(options, context, func));

			app.UseWebApi(CreateHttpCfg(options));


			return app;
		}

		private static async Task UpgradeToWebSockets(RavenDBOptions options, IOwinContext context, Func<Task> next)
		{
			var accept = context.Get<Action<IDictionary<string, object>, Func<IDictionary<string, object>, Task>>>("websocket.Accept");
			if (accept == null)
			{
				// Not a websocket request
				await next();
				return;
			}

			WebSocketsTransport webSocketsTrasport = WebSocketTransportFactory.CreateWebSocketTransport(options, context);

			if (webSocketsTrasport != null)
			{
				if (await webSocketsTrasport.TrySetupRequest())
					accept(null, webSocketsTrasport.Run);
			}
		}



		private static HttpConfiguration CreateHttpCfg(RavenDBOptions options)
		{
			var cfg = new HttpConfiguration(httpRouteCollection ?? new HttpRouteCollection());


			cfg.Properties[typeof (DatabasesLandlord)] = options.DatabaseLandlord;
			cfg.Properties[typeof (FileSystemsLandlord)] = options.FileSystemLandlord;
			cfg.Properties[typeof (CountersLandlord)] = options.CountersLandlord;
			cfg.Properties[typeof (MixedModeRequestAuthorizer)] = options.MixedModeRequestAuthorizer;
			cfg.Properties[typeof (RequestManager)] = options.RequestManager;
			cfg.Formatters.Remove(cfg.Formatters.XmlFormatter);
			cfg.Formatters.JsonFormatter.SerializerSettings.Converters.Add(new NaveValueCollectionJsonConverterOnlyForConfigFormatters());
			cfg.Services.Replace(typeof (IAssembliesResolver), new MyAssemblyResolver());
			cfg.Filters.Add(new RavenExceptionFilterAttribute());
			cfg.MapHttpAttributeRoutes(new RavenInlineConstraintResolver());

			cfg.Routes.MapHttpRoute(
				"RavenFs", "fs/{controller}/{action}",
				new {id = RouteParameter.Optional});

			cfg.Routes.MapHttpRoute(
				"API Default", "{controller}/{action}",
				new {id = RouteParameter.Optional});

			cfg.Routes.MapHttpRoute(
				"Database Route", "databases/{databaseName}/{controller}/{action}",
				new {id = RouteParameter.Optional});

			cfg.MessageHandlers.Add(new ThrottlingHandler(options.SystemDatabase.Configuration.MaxConcurrentServerRequests));
			cfg.MessageHandlers.Add(new GZipToJsonAndCompressHandler());

			cfg.Services.Replace(typeof (IHostBufferPolicySelector), new SelectiveBufferPolicySelector());
			cfg.EnsureInitialized();

			return cfg;
		}

		private class MyAssemblyResolver : IAssembliesResolver
		{
			public ICollection<Assembly> GetAssemblies()
			{
				return AppDomain.CurrentDomain.GetAssemblies()
					.Where(a => !a.IsDynamic && a.ExportedTypes.Any(t => t.IsSubclassOf(typeof(RavenBaseApiController))))
					.ToArray();
			}
		}

		public class SelectiveBufferPolicySelector : IHostBufferPolicySelector
		{
			public bool UseBufferedInputStream(object hostContext)
			{
				var context = hostContext as IOwinContext;

				if (context != null)
				{
					if (context.Request.Uri.LocalPath.EndsWith("bulkInsert", StringComparison.OrdinalIgnoreCase) ||
						context.Request.Uri.LocalPath.EndsWith("studio-tasks/loadCsvFile", StringComparison.OrdinalIgnoreCase) ||
						context.Request.Uri.LocalPath.EndsWith("studio-tasks/import", StringComparison.OrdinalIgnoreCase) ||
						context.Request.Uri.LocalPath.EndsWith("replication/replicateDocs", StringComparison.OrdinalIgnoreCase) ||
						context.Request.Uri.LocalPath.EndsWith("replication/replicateAttachments", StringComparison.OrdinalIgnoreCase))
						return false;
				}

				return true;
			}

			public bool UseBufferedOutputStream(HttpResponseMessage response)
			{
				var content = response.Content;
				var compressedContent = content as GZipToJsonAndCompressHandler.CompressedContent;
				if (compressedContent != null && response.StatusCode != HttpStatusCode.NoContent)
					return ShouldBuffer(compressedContent.OriginalContent);
				return ShouldBuffer(content);
			}

			private bool ShouldBuffer(HttpContent content)
			{
				return (content is IEventsTransport ||
						content is StreamsController.StreamQueryContent ||
						content is StreamContent ||
						content is PushStreamContent ||
						content is JsonContent ||
						content is MultiGetController.MultiGetContent) == false;
			}
		}

		private class InterceptMiddleware : OwinMiddleware
		{
			public InterceptMiddleware(OwinMiddleware next)
				: base(next)
			{
			}

			public override async Task Invoke(IOwinContext context)
			{
				// Pre request stuff
				await Next.Invoke(context);
				// Post request stuff
			}
		}
	}
}
