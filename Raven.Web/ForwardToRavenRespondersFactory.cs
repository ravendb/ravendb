//-----------------------------------------------------------------------
// <copyright file="ForwardToRavenRespondersFactory.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading;
using System.Web;
using System.Web.Hosting;
using Raven.Abstractions.Logging;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;

namespace Raven.Web
{
    using Raven.Web.Utils;

    public class ForwardToRavenRespondersFactory : IHttpHandlerFactory
    {
        internal static DocumentDatabase database;
        internal static HttpServer server;

        internal static ShutdownDetector shutdownDetector;

        private static readonly object locker = new object();

        private static ILog log = LogManager.GetCurrentClassLogger();

        public IHttpHandler GetHandler(HttpContext context, string requestType, string url, string pathTranslated)
        {
            if (database == null)
                throw new InvalidOperationException("Database has not been initialized properly");
            if (server == null)
                throw new InvalidOperationException("Server has not been initialized properly");

            var reqUrl = UrlExtension.GetRequestUrlFromRawUrl(context.Request.RawUrl, database.Configuration);

            if (HttpServer.ChangesQuery.IsMatch(reqUrl))
                return new ChangesCurrentDatabaseForwardingHandler(server, shutdownDetector.Token);

            return new ForwardToRavenResponders(server);
        }

        public void ReleaseHandler(IHttpHandler handler)
        {
        }

        public static void Init()
        {
            if (database != null)
                return;

            lock (locker)
            {
                if (database != null)
                    return;

                log.Info("Setting up RavenDB Http Integration to the ASP.Net Pipeline");
                try
                {
                    var ravenConfiguration = new RavenConfiguration();
                    HttpEndpointRegistration.RegisterHttpEndpointTarget();
                    database = new DocumentDatabase(ravenConfiguration);
                    database.SpinBackgroundWorkers();
                    server = new HttpServer(ravenConfiguration, database);
                    server.Init();
                }
                catch
                {
                    if (database != null)
                    {
                        database.Dispose();
                        database = null;
                    }
                    if (server != null)
                    {
                        server.Dispose();
                        server = null;
                    }
                    throw;
                }

                shutdownDetector = new ShutdownDetector(log);
                shutdownDetector.Initialize();

                shutdownDetector.Token.Register(OnShutdown);
            }
        }

        private static void OnShutdown()
        {
            try
            {
                lock (locker)
                {
                    Thread.MemoryBarrier();
                    Shutdown();
                }
            }
            catch (Exception e)
            {
                GC.KeepAlive(e);
            }
        }

        public static void Shutdown()
        {
            log.Info("Disposing of RavenDB Http Integration to the ASP.Net Pipeline");
            if (server != null)
                server.Dispose();

            if (database != null)
                database.Dispose();

            server = null;
            database = null;

        }
    }
}