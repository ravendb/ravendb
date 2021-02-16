using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.WebSockets;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.Dispatcher;
using System.Web.Http.Hosting;
using Microsoft.Owin;
using Rachis;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Database.Config;
using Raven.Database.DiskIO;
using Raven.Database.Raft;
using Raven.Database.FileSystem.Util;
using Raven.Database.Server;
using Raven.Database.Server.Connections;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi;
using Raven.Database.Server.WebApi.Filters;
using Raven.Database.Server.WebApi.Handlers;

// ReSharper disable once CheckNamespace
using Raven.Abstractions.Logging;
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

            AssemblyExtractor.ExtractEmbeddedAssemblies(options.SystemDatabase.Configuration);

#if DEBUG
            app.UseInterceptor();
#endif

            app.Use((context, func) => UpgradeToWebSockets(options, context, func));

            app.Use<CustomExceptionMiddleware>().UseWebApi(CreateHttpCfg(options));


            return app;
        }

        private static async Task UpgradeToWebSockets(RavenDBOptions options, IOwinContext context, Func<Task> next)
        {
            var accept = context.Get<Action<IDictionary<string, object>, Func<IDictionary<string, object>, Task>>>("websocket.Accept");
            if (accept == null)
            {
                // Not a websocket request
                await next().ConfigureAwait(false);
                return;
            }

            WebSocketsTransport webSocketsTrasport = WebSocketTransportFactory.CreateWebSocketTransport(options, context);

            if (webSocketsTrasport != null)
            {
                if (await webSocketsTrasport.TrySetupRequest().ConfigureAwait(false))
                {
                    accept(new Dictionary<string, object>()
                    {
                        {"websocket.ReceiveBufferSize", 256},
                        {"websocket.Buffer", webSocketsTrasport.PreAllocatedBuffer},
                        {"websocket.KeepAliveInterval", WebSocket.DefaultKeepAliveInterval}
                    }, webSocketsTrasport.Run);
            }
        }
        }

        private static HttpConfiguration CreateHttpCfg(RavenDBOptions options)
        {
            var cfg = new HttpConfiguration();

            cfg.Properties[typeof(DatabasesLandlord)] = options.DatabaseLandlord;
            cfg.Properties[typeof(FileSystemsLandlord)] = options.FileSystemLandlord;
            cfg.Properties[typeof(CountersLandlord)] = options.CountersLandlord;
            cfg.Properties[typeof(TimeSeriesLandlord)] = options.TimeSeriesLandlord;
            cfg.Properties[typeof(MixedModeRequestAuthorizer)] = options.MixedModeRequestAuthorizer;
            cfg.Properties[typeof(RequestManager)] = options.RequestManager;
            cfg.Properties[typeof(ClusterManager)] = options.ClusterManager;
            cfg.Properties[Constants.MaxConcurrentRequestsForDatabaseDuringLoad] = new SemaphoreSlim(options.SystemDatabase.Configuration.MaxConcurrentRequestsForDatabaseDuringLoad);
            cfg.Properties[Constants.MaxSecondsForTaskToWaitForDatabaseToLoad] = options.SystemDatabase.Configuration.MaxSecondsForTaskToWaitForDatabaseToLoad;
            cfg.Formatters.Remove(cfg.Formatters.XmlFormatter);
            cfg.Formatters.JsonFormatter.SerializerSettings.Converters.Add(new NaveValueCollectionJsonConverterOnlyForConfigFormatters());
            cfg.Services.Replace(typeof(IAssembliesResolver), new RavenAssemblyResolver());
            cfg.Filters.Add(new RavenExceptionFilterAttribute());

            if (options.SystemDatabase.Configuration.MaxConcurrentServerRequests >= 0)
                cfg.MessageHandlers.Add(new ThrottlingHandler(options.SystemDatabase.Configuration.MaxConcurrentServerRequests));

            cfg.MessageHandlers.Add(new GZipToJsonAndCompressHandler());

            cfg.Services.Replace(typeof(IHostBufferPolicySelector), new SelectiveBufferPolicySelector());

            if (RouteCacher.TryAddRoutesFromCache(cfg) == false)
                AddRoutes(cfg);

            cfg.EnsureInitialized();

            RouteCacher.CacheRoutesIfNecessary(cfg);

            return cfg;
        }

        private static void AddRoutes(HttpConfiguration cfg)
        {
            cfg.MapHttpAttributeRoutes(new RavenInlineConstraintResolver());

            cfg.Routes.MapHttpRoute(
                "RavenFs", "fs/{controller}/{action}",
                new { id = RouteParameter.Optional });

            cfg.Routes.MapHttpRoute(
                "API Default", "{controller}/{action}",
                new { id = RouteParameter.Optional });

            cfg.Routes.MapHttpRoute(
                "Database Route", "databases/{databaseName}/{controller}/{action}",
                new { id = RouteParameter.Optional });
        }

        private class RavenAssemblyResolver : IAssembliesResolver
        {
            public ICollection<Assembly> GetAssemblies()
            {
                return AppDomain.CurrentDomain.GetAssemblies()
                    .Where(IsRavenAssembly)
                    .ToArray();
            }

            private static bool IsRavenAssembly(Assembly assembly)
            {
                if (assembly.IsDynamic)
                    return false;

                try
                {
                    return assembly.ExportedTypes.Any(t => t.IsSubclassOf(typeof (RavenBaseApiController)));
                }
                catch (FileLoadException)
                {
                    // if we can't figure out, this proably isn't it
                    return false;
                }
                catch (FileNotFoundException)
                {
                    //ExportedTypes will throw a FileNotFoundException if the assembly references another assembly which cannot be loaded/found
                    return false;
                }
            }
        }

        public class SelectiveBufferPolicySelector : IHostBufferPolicySelector
        {
            public bool UseBufferedInputStream(object hostContext)
            {
                var context = hostContext as IOwinContext;

                if (context != null)
                {
                    var pathString = context.Request.Path;
                    if (pathString.HasValue)
                    {
                        var localPath = pathString.Value;
                        var length = localPath.Length;
                        if (length < 10) // the shortest possible URL to consider here: fs/{fs_name_at_least_one_character}/files
                            return true;

                        var prev = localPath[length - 2];
                        switch (localPath[length-1])
                        {
                            case 't':
                            case 'T':
                                switch (prev)
                                {
                                    case 'R':
                                    case 'r':
                                        return (
                                            localPath.EndsWith("bulkInsert", StringComparison.OrdinalIgnoreCase) ||
                                            localPath.EndsWith("studio-tasks/import", StringComparison.OrdinalIgnoreCase)
                                            ) == false;
                                    default:
                                        return true;
                                        
                                }
                            case 'e':
                            case 'E':
                                switch (prev)
                                {
                                    case 'l':
                                    case 'L':
                                        return localPath.EndsWith("studio-tasks/loadCsvFile", StringComparison.OrdinalIgnoreCase) == false;
                                    default:
                                        return true;
                                }
                            case 'D':
                            case 'd':
                                switch (prev)
                                {
                                    case 'E':
                                    case 'e':
                                        return localPath.EndsWith("synchronization/MultipartProceed", StringComparison.OrdinalIgnoreCase) == false;
                                    default:
                                        return true;
                                }
                            case 's':
                            case 'S':
                                switch (prev)
                                {
                                    case 'T':
                                    case 't':
                                        return localPath.EndsWith("replication/replicateAttachments", StringComparison.OrdinalIgnoreCase) == false;
                                    case 'c':
                                    case 'C':
                                        if (localPath[length - 4] == '/')
                                        return true;
                                        return localPath.EndsWith("replication/replicateDocs", StringComparison.OrdinalIgnoreCase) == false;
                                    case 'E':
                                    case 'e':
                                        return localPath.EndsWith("files", StringComparison.OrdinalIgnoreCase) == false || context.Request.Method != "PUT";
                                    default:
                                        return true;
                                }
                            default:
                                return true;
                        }
                    }
                }

                return true;
            }

            public bool UseBufferedOutputStream(HttpResponseMessage response)
            {
                var content = response.Content;
                var compressedContent = content as CompressedContent;
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
            private static readonly ILog Log = LogManager.GetCurrentClassLogger();

            public InterceptMiddleware(OwinMiddleware next)
                : base(next)
            {
            }

            public override async Task Invoke(IOwinContext context)
            {
                // Pre request stuff
                try
                {
                    await Next.Invoke(context).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    if (Log.IsDebugEnabled)
                        Log.DebugException("Exception thrown while invoking message from client to server, probably due to write fail to a closed connection which may normally occur when browsing",ex);
                }
                // Post request stuff
            }
        }

        private class CustomExceptionMiddleware : OwinMiddleware
        {
            private static readonly ILog Log = LogManager.GetCurrentClassLogger();

            public CustomExceptionMiddleware(OwinMiddleware next) : base(next)
            {}

            public override async Task Invoke(IOwinContext context)
            {
                try
                {
                    await Next.Invoke(context).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    if (Log.IsDebugEnabled)
                        Log.DebugException("Exception thrown while invoking message from client to server, probably due to write fail to a closed connection which may normally occur when browsing",ex);
                }
            }
        }
    }
}
