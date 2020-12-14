//-----------------------------------------------------------------------
// <copyright file="RavenDbServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Lucene.Net.Search;
using Raven.Abstractions.Replication;
using Raven.Client.Document;
using Raven.Client.FileSystem;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Embedded;
using Raven.Database.FileSystem;
using Raven.Database.Server;
using Raven.Database.Server.WebApi;
using Raven.Database.Util;

namespace Raven.Server
{
    public class RavenDbServer : IDisposable
    {
        private readonly DocumentStore documentStore;
        internal readonly FilesStore filesStore;
        private readonly MetricsTicker metricsTicker;

        private InMemoryRavenConfiguration configuration;
        private IServerThingsForTests serverThingsForTests;
        private RavenDBOptions options;
        private OwinHttpServer owinHttpServer;

        private string url;

        private bool filesStoreInitialized;

        public RavenDbServer()
            : this(new RavenConfiguration())
        { }

        public RavenDbServer(InMemoryRavenConfiguration configuration)
        {
            this.configuration = configuration;

            documentStore = new DocumentStore
            {
                Conventions =
                {
                    FailoverBehavior = FailoverBehavior.FailImmediately
                }
            };
            filesStore = new FilesStore
            {
                Conventions =
                {
                    FailoverBehavior = FailoverBehavior.FailImmediately
                }
            };

            metricsTicker = MetricsTicker.Instance;

        }

        public InMemoryRavenConfiguration Configuration
        {
            get { return configuration; }
            set { configuration = value; }
        }

        //TODO http://issues.hibernatingrhinos.com/issue/RavenDB-1451
        public DocumentDatabase SystemDatabase
        {
            get
            {
                if (options == null)
                    return null;
                return options.SystemDatabase;
            }
        }

        //TODO http://issues.hibernatingrhinos.com/issue/RavenDB-1451
        public IServerThingsForTests Server
        {
            get { return serverThingsForTests; }
        }

        public DocumentStore DocumentStore
        {
            get { return documentStore; }
        }

        public FilesStore FilesStore
        {
            get
            {
                if (filesStoreInitialized)
                    return filesStore;

                lock (filesStore)
                {
                    if (filesStoreInitialized)
                        return filesStore;

                    filesStoreInitialized = true;
                    filesStore.Initialize();
                }

                return filesStore;
            }
        }

        public bool RunInMemory
        {
            get { return configuration.RunInMemory; }
            set { configuration.RunInMemory = value; }
        }

        public RavenDbServer Initialize(Action<RavenDBOptions> configure = null)
        {
            if (configuration.IgnoreSslCertificateErrors == IgnoreSslCertificateErrorsMode.All)
            {
                // we ignore either all or none at the moment
                ServicePointManager.ServerCertificateValidationCallback = (sender, certificate, chain, errors) => true;
            }

            BooleanQuery.MaxClauseCount = configuration.MaxClauseCount;

            owinHttpServer = new OwinHttpServer(configuration, useHttpServer: UseEmbeddedHttpServer, configure: configure);
            options = owinHttpServer.Options;

            serverThingsForTests = new ServerThingsForTests(options);
            Func<HttpMessageHandler> httpMessageHandlerFactory = () => new OwinClientHandler(owinHttpServer.Invoke, options.SystemDatabase.Configuration.EnableResponseLoggingForEmbeddedDatabases, options.SystemDatabase.Configuration.EmbeddedResponseStreamMaxCachedBlocks);
            documentStore.HttpMessageHandlerFactory = httpMessageHandlerFactory;
            documentStore.Url = string.IsNullOrWhiteSpace(Url) ? "http://localhost" : Url;
            documentStore.Initialize();

            filesStore.HttpMessageHandlerFactory = httpMessageHandlerFactory;
            filesStore.Url = string.IsNullOrWhiteSpace(Url) ? "http://localhost" : Url;

            return this;
        }

        public void EnableHttpServer()
        {
            owinHttpServer.EnableHttpServer(configuration);
        }

        public void DisableHttpServer()
        {
            owinHttpServer.DisableHttpServer();
        }

        public RavenDBOptions Options
        {
            get { return options; }
        }

        public string Url
        {
            get { return url; }
            set { url = value.EndsWith("/") ? value.Substring(0, value.Length - 1) : value; }
        }

        ///<summary>
        /// Whatever we should also host an HTTP endpoint for the document database
        ///</summary>
        public bool UseEmbeddedHttpServer { get; set; }

        public bool Disposed { get; private set; }

        public void Dispose()
        {
            if (Disposed)
                return;

            Disposed = true;

            if (metricsTicker != null)
                metricsTicker.Dispose();

            if (documentStore != null)
                documentStore.Dispose();

            if (filesStore != null)
                filesStore.Dispose();

            if (owinHttpServer != null)
                owinHttpServer.Dispose();

            if (configuration != null)
                configuration.Dispose();
        }

        //TODO http://issues.hibernatingrhinos.com/issue/RavenDB-1451
        private class ServerThingsForTests : IServerThingsForTests
        {
            private readonly RavenDBOptions options;

            public ServerThingsForTests(RavenDBOptions options)
            {
                this.options = options;
            }

            public bool HasPendingRequests
            {
                get { return false; } //TODO DH: fix (copied from WebApiServer)
            }

            public int NumberOfRequests
            {
                get { return options.RequestManager.NumberOfRequests; }
            }
            public RavenDBOptions Options { get { return options; } }

            public void ResetNumberOfRequests()
            {
                options.RequestManager.ResetNumberOfRequests();
            }

            public Task<DocumentDatabase> GetDatabaseInternal(string databaseName)
            {
                return options.DatabaseLandlord.GetResourceInternal(databaseName);
            }

            public Task<RavenFileSystem> GetRavenFileSystemInternal(string fileSystemName)
            {
                return options.FileSystemLandlord.GetResourceInternal(fileSystemName);
            }

            public RequestManager RequestManager { get { return options.RequestManager; } }
        }
    }

    //TODO http://issues.hibernatingrhinos.com/issue/RavenDB-1451
    public interface IServerThingsForTests
    {
        bool HasPendingRequests { get; }
        int NumberOfRequests { get; }
        RavenDBOptions Options { get; }
        void ResetNumberOfRequests();
        Task<DocumentDatabase> GetDatabaseInternal(string databaseName);
        Task<RavenFileSystem> GetRavenFileSystemInternal(string fileSystemName);

        RequestManager RequestManager { get; }
    }
}
