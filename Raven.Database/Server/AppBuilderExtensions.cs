using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.Dispatcher;
using System.Web.Http.Hosting;
using Microsoft.Owin;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Logging;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Server.Connections;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.RavenFS;
using Raven.Database.Server.RavenFS.Util;
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

            if (context.Request.Uri.LocalPath.EndsWith("changes/websocket"))
            {
                var webSocketsTrasport = new WebSocketsTransport(options, context);
                if (await webSocketsTrasport.TrySetupRequest())
                    accept(null, webSocketsTrasport.Run);
            }
            else if (context.Request.Uri.LocalPath.EndsWith("http-trace/websocket"))
            {
                var webSocketsTrasport = new WebSocketsTransport(options, context,  (resourceName, currentWebsocketTransport, user) =>
                {
                    if (resourceName != null)
                    {
                        options.RequestManager.RegisterResourceHttpTraceTransport(currentWebsocketTransport, resourceName);
                    }
                    else
                    {
                        var oneTimetokenPrincipal = user as MixedModeRequestAuthorizer.OneTimetokenPrincipal;

                        if ((oneTimetokenPrincipal != null && oneTimetokenPrincipal.IsAdministratorInAnonymouseMode) ||
                            options.SystemDatabase.Configuration.AnonymousUserAccessMode == AnonymousUserAccessMode.Admin)
                        {
                            options.RequestManager.RegisterServerHttpTraceTransport(currentWebsocketTransport);
                        }
                        else
                        {
                            context.Response.StatusCode = 403;
                            context.Response.ReasonPhrase = "Forbidden";
                            context.Response.Write("{'Error': 'Administrator user is required in order to trace the whole server' }");
                            return false;
                        }
                    }
                    return true;
                });
                if (await webSocketsTrasport.TrySetupRequest())
                    accept(null, webSocketsTrasport.Run);
            }
            else if (context.Request.Uri.LocalPath.EndsWith("admin/logs/events"))
            {
                /*public class LogEventInfoFormatted
                    {
                        public String Level { get; set; }
                        public DateTime TimeStamp { get; set; }
                        public string Message { get; set; }
                        public string LoggerName { get; set; }
                        public string Exception { get; set; }

                        public LogEventInfoFormatted(LogEventInfo eventInfo)
                        {
                            TimeStamp = eventInfo.TimeStamp;
                            Message = eventInfo.FormattedMessage;
                            LoggerName = eventInfo.LoggerName;
                            Level = eventInfo.Level.ToString();
                            Exception = eventInfo.Exception == null ? null : eventInfo.Exception.ToString();
                        }
                    }*/
                var webSocketsTrasport = new WebSocketsTransport(options, context, (resourceName, currentWebsocketTransport, user) =>
                {
                    try
                    {
                        var oneTimetokenPrincipal = user as MixedModeRequestAuthorizer.OneTimetokenPrincipal;

                        if ((oneTimetokenPrincipal == null || !oneTimetokenPrincipal.IsAdministratorInAnonymouseMode) &&
                            options.SystemDatabase.Configuration.AnonymousUserAccessMode != AnonymousUserAccessMode.Admin)
                        {
                            context.Response.StatusCode = 403;
                            context.Response.ReasonPhrase = "Forbidden";
                            context.Response.Write("{'Error': 'Administrator user is required in order to trace the whole server' }");
                            return false;
                        }

                        var logTarget = LogManager.GetTarget<OnDemandLogTarget>();
                        logTarget.Register(currentWebsocketTransport);
                        return true;
                    }
                    catch 
                    {

                        return false;
                    }
                    
                }, (message) =>
                {
                    var typedMessage = message as LogEventInfo;
                    if (typedMessage != null)
                    {
                        var formattedMessage = new LogEventInfoFormatted(typedMessage);
                        return formattedMessage;
                    }
                    return message;
                    
                });
                if (await webSocketsTrasport.TrySetupRequest())
                    accept(null, webSocketsTrasport.Run);
            }
        }
		private static HttpConfiguration CreateHttpCfg(RavenDBOptions options)
		{
			var cfg = new HttpConfiguration();
			cfg.Properties[typeof(DatabasesLandlord)] = options.DatabaseLandlord;
            cfg.Properties[typeof(FileSystemsLandlord)] = options.FileSystemLandlord;
			cfg.Properties[typeof(CountersLandlord)] = options.CountersLandlord;
			cfg.Properties[typeof(MixedModeRequestAuthorizer)] = options.MixedModeRequestAuthorizer;
			cfg.Properties[typeof(RequestManager)] = options.RequestManager;
			cfg.Formatters.Remove(cfg.Formatters.XmlFormatter);
			cfg.Formatters.JsonFormatter.SerializerSettings.Converters.Add(new NaveValueCollectionJsonConverterOnlyForConfigFormatters());

			cfg.Services.Replace(typeof(IAssembliesResolver), new MyAssemblyResolver());
			cfg.Filters.Add(new RavenExceptionFilterAttribute());
			cfg.MapHttpAttributeRoutes();

			cfg.Routes.MapHttpRoute(
				"RavenFs", "fs/{controller}/{action}",
				new {id = RouteParameter.Optional});

			cfg.Routes.MapHttpRoute(
				"API Default", "{controller}/{action}",
				new { id = RouteParameter.Optional });

			cfg.Routes.MapHttpRoute(
				"Database Route", "databases/{databaseName}/{controller}/{action}",
				new { id = RouteParameter.Optional });

			cfg.MessageHandlers.Add(new GZipToJsonAndCompressHandler());

			cfg.Services.Replace(typeof(IHostBufferPolicySelector), new SelectiveBufferPolicySelector());
			cfg.EnsureInitialized();
			return cfg;
		}

        

		private class MyAssemblyResolver : IAssembliesResolver
		{
			public ICollection<Assembly> GetAssemblies()
			{
				return AppDomain.CurrentDomain.GetAssemblies().ToList(); ;
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
		        return (content is ChangesPushContent ||
                        content is LogsPushContent ||
		                content is StreamsController.StreamQueryContent ||
		                content is StreamContent ||
		                content is PushStreamContent ||
		                content is JsonContent ||
		                content is MultiGetController.MultiGetContent) == false;
		    }
		}

		private class InterceptMiddleware : OwinMiddleware
		{
			public InterceptMiddleware(OwinMiddleware next) : base(next)
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
