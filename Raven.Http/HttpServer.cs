//-----------------------------------------------------------------------
// <copyright file="HttpServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;
using log4net;
using Newtonsoft.Json;
using System.Linq;
using Raven.Http.Abstractions;
using Raven.Http.Exceptions;
using Raven.Http.Extensions;
using Formatting = Newtonsoft.Json.Formatting;

namespace Raven.Http
{
    public abstract class HttpServer : IDisposable
    {
        protected readonly IResourceStore DefaultResourceStore;
        protected readonly IRaveHttpnConfiguration DefaultConfiguration;

        private readonly ThreadLocal<string> currentTenantId = new ThreadLocal<string>();
        private readonly ThreadLocal<IResourceStore> currentDatabase = new ThreadLocal<IResourceStore>(()=>null);
        private readonly ThreadLocal<IRaveHttpnConfiguration> currentConfiguration = new ThreadLocal<IRaveHttpnConfiguration>(()=>null);

        protected readonly ConcurrentDictionary<string, IResourceStore> ResourcesStoresCache =
            new ConcurrentDictionary<string, IResourceStore>(StringComparer.InvariantCultureIgnoreCase);

        private readonly ConcurrentDictionary<string, DateTime> databaseLastRecentlyUsed = new ConcurrentDictionary<string, DateTime>();


        [ImportMany]
        public IEnumerable<AbstractRequestResponder> RequestResponders { get; set; }

        public IRaveHttpnConfiguration Configuration
        {
            get
            {
                return DefaultConfiguration;
            }
        }

        public abstract Regex TenantsQuery { get; }

        private HttpListener listener;

        private readonly ILog logger = LogManager.GetLogger(typeof(HttpServer));

        private int reqNum;


        // concurrent requests
        // we set 1/4 aside for handling background tasks
        private readonly SemaphoreSlim concurretRequestSemaphore = new SemaphoreSlim(192);
        private Timer databasesCleanupTimer;

        protected HttpServer(IRaveHttpnConfiguration configuration, IResourceStore resourceStore)
        {
            DefaultResourceStore = resourceStore;
            DefaultConfiguration = configuration;

            configuration.Container.SatisfyImportsOnce(this);

            foreach (var requestResponder in RequestResponders)
            {
                requestResponder.Initialize(() => currentDatabase.Value, () => currentConfiguration.Value);
            }
        }

        #region IDisposable Members

        public void Dispose()
        {
            databasesCleanupTimer.Dispose();
            if (listener != null && listener.IsListening)
                listener.Stop();
            foreach (var documentDatabase in ResourcesStoresCache)
            {
                documentDatabase.Value.Dispose();
            }
        }

        #endregion

        public void Start()
        {
            listener = new HttpListener();
            string virtualDirectory = DefaultConfiguration.VirtualDirectory;
            if (virtualDirectory.EndsWith("/") == false)
                virtualDirectory = virtualDirectory + "/";
            listener.Prefixes.Add("http://" + (DefaultConfiguration.HostName ?? "+") + ":" + DefaultConfiguration.Port + virtualDirectory);
            switch (DefaultConfiguration.AnonymousUserAccessMode)
            {
                case AnonymousUserAccessMode.None:
                    listener.AuthenticationSchemes = AuthenticationSchemes.IntegratedWindowsAuthentication;
                    break;
                case AnonymousUserAccessMode.All:
                    break;
                case AnonymousUserAccessMode.Get:
                    listener.AuthenticationSchemes = AuthenticationSchemes.IntegratedWindowsAuthentication |
                        AuthenticationSchemes.Anonymous;
                    listener.AuthenticationSchemeSelectorDelegate = request =>
                    {
                        return request.HttpMethod == "GET" || request.HttpMethod == "HEAD" ?
                            AuthenticationSchemes.Anonymous :
                            AuthenticationSchemes.IntegratedWindowsAuthentication;
                    };
                    break;
                default:
                    throw new ArgumentException("Cannot understand access mode: " + DefaultConfiguration.AnonymousUserAccessMode);
            }
            databasesCleanupTimer = new Timer(CleanupDatabases, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            listener.Start();
            listener.BeginGetContext(GetContext, null);
        }

        private void CleanupDatabases(object state)
        {
            var databasesToCleanup = databaseLastRecentlyUsed
                .Where(x=>(DateTime.Now - x.Value).TotalMinutes > 10)
                .Select(x=>x.Key)
                .ToArray();

            foreach (var db in databasesToCleanup)
            {
                DateTime _;
                databaseLastRecentlyUsed.TryRemove(db, out _);

                IResourceStore database;
                if(ResourcesStoresCache.TryRemove(db, out database))
                    database.Dispose();
            }
        }

        private void GetContext(IAsyncResult ar)
        {
            IHttpContext ctx;
            try
            {
                ctx = new HttpListenerContextAdpater(listener.EndGetContext(ar), DefaultConfiguration);
                //setup waiting for the next request
                listener.BeginGetContext(GetContext, null);
            }
            catch (InvalidOperationException)
            {
                // can't get current request / end new one, probably
                // listner shutdown
                return;
            }
            catch (HttpListenerException)
            {
                // can't get current request / end new one, probably
                // listner shutdown
                return;
            }

            if (concurretRequestSemaphore.Wait(TimeSpan.FromSeconds(5)) == false)
            {
                HandleTooBusyError(ctx);
                return;
            }
            try
            {
                HandleActualRequest(ctx);
            }
            finally
            {
                concurretRequestSemaphore.Release();
            }
        }

        public void HandleActualRequest(IHttpContext ctx)
        {
            var sw = Stopwatch.StartNew();
            bool ravenUiRequest = false;
            try
            {
                ravenUiRequest = DispatchRequest(ctx);
            }
            catch (Exception e)
            {
                HandleException(ctx, e);
                logger.Warn("Error on request", e);
            }
            finally
            {
                ctx.FinalizeResonse();
                // we filter out requests for the UI because they fill the log with information
                // we probably don't care about
                if (ravenUiRequest == false)
                {
                    var curReq = Interlocked.Increment(ref reqNum);
                    logger.DebugFormat("Request #{0,4:#,0}: {1,-7} - {2,5:#,0} ms - {5,-10} - {3} - {4}",
                                       curReq, ctx.Request.HttpMethod, sw.ElapsedMilliseconds, ctx.Response.StatusCode,
                                       ctx.Request.Url.PathAndQuery,
                                       currentTenantId.Value);

					ctx.OutputSavedLogItems(logger);

                }
            }
        }

        private void HandleException(IHttpContext ctx, Exception e)
        {
            try
            {
                if (e is BadRequestException)
                    HandleBadRequest(ctx, (BadRequestException)e);
                else if (e is ConcurrencyException)
                    HandleConcurrencyException(ctx, (ConcurrencyException)e);
                else if (TryHandleException(ctx, e))
                    return;
                else
                    HandleGenericException(ctx, e);
            }
            catch (Exception)
            {
                logger.Error("Failed to properly handle error, further error handling is ignored", e);
            }
        }

        protected abstract bool TryHandleException(IHttpContext ctx, Exception exception);

       
        private static void HandleTooBusyError(IHttpContext ctx)
        {
            ctx.Response.StatusCode = 503;
            ctx.Response.StatusDescription = "Service Unavailable";
            SerializeError(ctx, new
            {
                Url = ctx.Request.RawUrl,
                Error = "The server is too busy, could not acquire transactional access"
            });
        }


        private static void HandleGenericException(IHttpContext ctx, Exception e)
        {
            ctx.Response.StatusCode = 500;
            ctx.Response.StatusDescription = "Internal Server Error";
            SerializeError(ctx, new
            {
                Url = ctx.Request.RawUrl,
                Error = e.ToString()
            });
        }

        private static void HandleBadRequest(IHttpContext ctx, BadRequestException e)
        {
            ctx.SetStatusToBadRequest();
            SerializeError(ctx, new
            {
                Url = ctx.Request.RawUrl,
                e.Message,
                Error = e.Message
            });
        }

        private static void HandleConcurrencyException(IHttpContext ctx, ConcurrencyException e)
        {
            ctx.Response.StatusCode = 409;
            ctx.Response.StatusDescription = "Conflict";
            SerializeError(ctx, new
            {
                Url = ctx.Request.RawUrl,
                e.ActualETag,
                e.ExpectedETag,
                Error = e.Message
            });
        }

        protected static void SerializeError(IHttpContext ctx, object error)
        {
            var sw = new StreamWriter(ctx.Response.OutputStream);
            new JsonSerializer().Serialize(new JsonTextWriter(sw)
            {
                Formatting = Formatting.Indented,
            }, error);
            sw.Flush();
        }

        private bool DispatchRequest(IHttpContext ctx)
        {
            if (AssertSecurityRights(ctx) == false)
                return false;

            SetupRequestToProperDatabase(ctx);
            CurrentOperationContext.Headers.Value = ctx.Request.Headers;
            try
            {
                OnDispatchingRequest(ctx);

                if (DefaultConfiguration.HttpCompression)
                    AddHttpCompressionIfClientCanAcceptIt(ctx);

                AddAccessControlAllowOriginHeader(ctx);

                foreach (var requestResponder in RequestResponders)
                {
                    if (requestResponder.WillRespond(ctx))
                    {
                        requestResponder.Respond(ctx);
                        return requestResponder.IsUserInterfaceRequest;
                    }
                }
                ctx.SetStatusToBadRequest();
                if (ctx.Request.HttpMethod == "HEAD")
                    return false;
                ctx.Write(
                    @"
<html>
    <body>
        <h1>Could not figure out what to do</h1>
        <p>Your request didn't match anything that Raven knows to do, sorry...</p>
    </body>
</html>
");
            }
            finally
            {
                CurrentOperationContext.Headers.Value = null;
                currentDatabase.Value = DefaultResourceStore;
                currentConfiguration.Value = DefaultConfiguration;
            }
            return true;
        }

        protected virtual void OnDispatchingRequest(IHttpContext ctx){}

        private void SetupRequestToProperDatabase(IHttpContext ctx)
        {
            var requestUrl = ctx.GetRequestUrl();
            var match = TenantsQuery.Match(requestUrl);

            IResourceStore resourceStore;
            if (match.Success == false)
            {
                currentTenantId.Value = "<default>";
                currentDatabase.Value = DefaultResourceStore;
                currentConfiguration.Value = DefaultConfiguration;
            } 
            else
            {
                var tenantId = match.Groups[1].Value;
                if(TryGetOrCreateResourceStore(tenantId, out resourceStore))
                {
                    databaseLastRecentlyUsed.AddOrUpdate(tenantId, DateTime.Now, (s, time) => DateTime.Now);
                    ctx.AdjustUrl(match.Value);
                    currentTenantId.Value = tenantId;
                    currentDatabase.Value = resourceStore;
                    currentConfiguration.Value = resourceStore.Configuration;
                }
                else
                {
                    throw new BadRequestException("Could not find a database named: " + tenantId);
                }
            }
        }

        protected abstract bool TryGetOrCreateResourceStore(string name, out IResourceStore database);


        private void AddAccessControlAllowOriginHeader(IHttpContext ctx)
        {
            if (string.IsNullOrEmpty(DefaultConfiguration.AccessControlAllowOrigin))
                return;
        	ctx.Response.AddHeader("Access-Control-Allow-Origin", DefaultConfiguration.AccessControlAllowOrigin);
        }

        private static void AddHttpCompressionIfClientCanAcceptIt(IHttpContext ctx)
        {
            var acceptEncoding = ctx.Request.Headers["Accept-Encoding"];

            if (string.IsNullOrEmpty(acceptEncoding))
                return;

            // gzip must be first, because chrome has an issue accepting deflate data
            // when sending it json text
            if ((acceptEncoding.IndexOf("gzip", StringComparison.InvariantCultureIgnoreCase) != -1))
            {
                ctx.SetResponseFilter(s => new GZipStream(s, CompressionMode.Compress, true));
                ctx.Response.AddHeader("Content-Encoding","gzip");
            }
            else if (acceptEncoding.IndexOf("deflate", StringComparison.InvariantCultureIgnoreCase) != -1)
            {
                ctx.SetResponseFilter(s => new DeflateStream(s, CompressionMode.Compress, true));
            	ctx.Response.AddHeader("Content-Encoding", "deflate");
            }

        }

        private bool AssertSecurityRights(IHttpContext ctx)
        {
            if (DefaultConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Get &&
                (ctx.User == null || ctx.User.Identity == null || ctx.User.Identity.IsAuthenticated == false) &&
                    (ctx.Request.HttpMethod != "GET" && ctx.Request.HttpMethod != "HEAD")
                )
            {
                ctx.SetStatusToUnauthorized();
                return false;
            }
            return true;
        }
    }
}
