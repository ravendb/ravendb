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
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Responders;
using Raven.Database.Json;
using System.Linq;

namespace Raven.Database.Server
{
    public class HttpServer : IDisposable
    {
        private readonly DocumentDatabase defaultDatabase;
        private readonly RavenConfiguration defaultConfiguration;

        private readonly ThreadLocal<DocumentDatabase> currentDatabase = new ThreadLocal<DocumentDatabase>();
        private readonly ThreadLocal<InMemroyRavenConfiguration> currentConfiguration = new ThreadLocal<InMemroyRavenConfiguration>();

        private readonly ConcurrentDictionary<string, DocumentDatabase> databaseCache =
            new ConcurrentDictionary<string, DocumentDatabase>();

        private readonly ConcurrentDictionary<string, DateTime> databaseLastRecentlyUsed = new ConcurrentDictionary<string, DateTime>();

        private static readonly Regex databaseQuery = new Regex("^/databases/([^/]+)(?=/?)");

        [ImportMany]
        public IEnumerable<RequestResponder> RequestResponders { get; set; }

        public InMemroyRavenConfiguration Configuration
        {
            get
            {
                return defaultConfiguration;
            }
        }

        private HttpListener listener;

        private readonly ILog logger = LogManager.GetLogger(typeof(HttpServer));

        private int reqNum;


        // concurrent requests
        // we set 1/4 aside for handling background tasks
        private readonly SemaphoreSlim concurretRequestSemaphore = new SemaphoreSlim(192);
        private Timer databasesCleanupTimer;

        public HttpServer(RavenConfiguration configuration, DocumentDatabase database)
        {
            defaultDatabase = database;
            defaultConfiguration = configuration;

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
            if (listener != null)
                listener.Stop();
            foreach (var documentDatabase in databaseCache)
            {
                documentDatabase.Value.Dispose();
            }
        }

        #endregion

        public void Start()
        {
            listener = new HttpListener();
            string virtualDirectory = defaultConfiguration.VirtualDirectory;
            if (virtualDirectory.EndsWith("/") == false)
                virtualDirectory = virtualDirectory + "/";
            listener.Prefixes.Add("http://" + (defaultConfiguration.HostName ?? "+") + ":" + defaultConfiguration.Port + virtualDirectory);
            switch (defaultConfiguration.AnonymousUserAccessMode)
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
                    throw new ArgumentException("Cannot understand access mode: " + defaultConfiguration.AnonymousUserAccessMode);
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

                DocumentDatabase database;
                if(databaseCache.TryRemove(db, out database))
                    database.Dispose();
            }
        }

        private void GetContext(IAsyncResult ar)
        {
            IHttpContext ctx;
            try
            {
                ctx = new HttpListenerContextAdpater(listener.EndGetContext(ar), defaultConfiguration);
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
                    logger.DebugFormat("Request #{0,4:#,0}: {1,-7} - {2,5:#,0} ms - {3} - {4}",
                                       curReq, ctx.Request.HttpMethod, sw.ElapsedMilliseconds, ctx.Response.StatusCode,
                                       ctx.Request.Url.PathAndQuery);
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
                else if (e is IndexDisabledException)
                    HandleIndexDisabledException(ctx, (IndexDisabledException)e);
                else if (e is IndexDoesNotExistsException)
                    HandleIndexDoesNotExistsException(ctx, e);
                else
                    HandleGenericException(ctx, e);
            }
            catch (Exception)
            {
                logger.Error("Failed to properly handle error, further error handling is ignored", e);
            }
        }

        private static void HandleIndexDoesNotExistsException(IHttpContext ctx, Exception e)
        {
            ctx.SetStatusToNotFound();
            SerializeError(ctx, new
            {
                Url = ctx.Request.RawUrl,
                Error = e.Message
            });
        }

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

        private static void HandleIndexDisabledException(IHttpContext ctx, IndexDisabledException e)
        {
            ctx.Response.StatusCode = 503;
            ctx.Response.StatusDescription = "Service Unavailable";
            SerializeError(ctx, new
            {
                Url = ctx.Request.RawUrl,
                Error = e.Information.GetErrorMessage(),
                Index = e.Information.Name,
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

        private static void SerializeError(IHttpContext ctx, object error)
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

            CurrentRavenOperation.Headers.Value = ctx.Request.Headers;
            try
            {


                if (defaultConfiguration.HttpCompression)
                    AddHttpCompressionIfClientCanAcceptIt(ctx);

                AddAccessControlAllowOriginHeader(ctx);

                foreach (var requestResponder in RequestResponders)
                {
                    if (requestResponder.WillRespond(ctx))
                    {
                        requestResponder.Respond(ctx);
                        return requestResponder is RavenUI || requestResponder is RavenRoot || requestResponder is Favicon;
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
                CurrentRavenOperation.Headers.Value = null;
                currentDatabase.Value = defaultDatabase;
                currentConfiguration.Value = defaultConfiguration;
            }
            return true;
        }

        private void SetupRequestToProperDatabase(IHttpContext ctx)
        {
            var requestUrl = ctx.GetRequestUrl();
            var match = databaseQuery.Match(requestUrl);

            DocumentDatabase database;
            if (match.Success == false)
            {
                currentDatabase.Value = defaultDatabase;
                currentConfiguration.Value = defaultConfiguration;
            } 
            else if(TryGetOrCreateDatabase(match.Groups[1].Value, out database))
            {
                databaseLastRecentlyUsed.AddOrUpdate(match.Groups[1].Value, DateTime.Now, (s, time) => DateTime.Now);
                ctx.AdjustUrl(match.Value);
                currentDatabase.Value = database;
                currentConfiguration.Value = database.Configuration;
            }
            else
            {
                throw new BadRequestException("Could not find a database named: " + match.Groups[1].Value);
            }
        }

        private bool TryGetOrCreateDatabase(string tenantId, out DocumentDatabase database)
        {
            if (databaseCache.TryGetValue(tenantId, out database))
                return true;

            var jsonDocument = defaultDatabase.Get("Raven/Databases/" + tenantId, null);

            if (jsonDocument == null)
                return false;

            var document = jsonDocument.DataAsJson.JsonDeserialization<DatabaseDocument>();

            database = databaseCache.GetOrAdd(tenantId, s =>
            {
                var config = new InMemroyRavenConfiguration
                {
                    Settings = defaultConfiguration.Settings,
                };
                config.Settings["Raven/VirtualDir"] = config.Settings["Raven/VirtualDir"] + "/" + tenantId;
                foreach (var setting in document.Settings)
                {
                    config.Settings[setting.Key] = setting.Value;
                }
                config.Initialize();
                return new DocumentDatabase(config);
            });
            return true;


        }


        private void AddAccessControlAllowOriginHeader(IHttpContext ctx)
        {
            if (string.IsNullOrEmpty(defaultConfiguration.AccessControlAllowOrigin))
                return;
            ctx.Response.Headers["Access-Control-Allow-Origin"] = defaultConfiguration.AccessControlAllowOrigin;
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
                ctx.Response.Headers["Content-Encoding"] = "gzip";
            }
            else if (acceptEncoding.IndexOf("deflate", StringComparison.InvariantCultureIgnoreCase) != -1)
            {
                ctx.SetResponseFilter(s => new DeflateStream(s, CompressionMode.Compress, true));
                ctx.Response.Headers["Content-Encoding"] = "deflate";
            }

        }

        private bool AssertSecurityRights(IHttpContext ctx)
        {
            if (defaultConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Get &&
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
