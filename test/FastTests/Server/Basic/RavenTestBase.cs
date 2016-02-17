using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Json;
using Raven.Server.Utils;

namespace Raven.Tests.Core
{
    public class RavenTestBase : IDisposable
    {
        public const string ServerName = "Raven.Tests.Core.Server";

        protected readonly List<DocumentStore> CreatedStores = new List<DocumentStore>();

        private static int _currentServerUsages;
        private static RavenServer _globalServer;
        private static readonly object ServerLocker = new object();
        private RavenServer _localServer;

        public RavenServer Server
        {
            get
            {
                if (_localServer != null)
                    return _localServer;
                lock (ServerLocker)
                {
                    if (_globalServer == null)
                        _globalServer = CreateServer();
                    _currentServerUsages++;
                    _localServer = _globalServer;
                }
                return _globalServer;
            }
        }

        private static RavenServer CreateServer()
        {
            var configuration = new RavenConfiguration();
            configuration.Initialize();

            configuration.Core.ServerUrl = "http://localhost:8080";
            configuration.Server.Name = ServerName;
            configuration.Core.RunInMemory = true;
            configuration.Core.DataDirectory = Path.Combine(configuration.Core.DataDirectory, "Tests");
            configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(10, TimeUnit.Seconds);
            configuration.Storage.AllowOn32Bits = true;

            IOExtensions.DeleteDirectory(configuration.Core.DataDirectory);

            var server = new RavenServer(configuration);
            server.Initialize();

            // TODO: Make sure to properly handle this when this is resolved:
            // TODO: https://github.com/dotnet/corefx/issues/5205
            // TODO: AssemblyLoadContext.GetLoadContext(typeof(RavenTestBase).GetTypeInfo().Assembly).Unloading +=

            return server;
        }


        protected virtual async Task<DocumentStore> GetDocumentStore([CallerMemberName] string databaseName = null, string dbSuffixIdentifier = null,
           Action<DatabaseDocument> modifyDatabaseDocument = null)
        {
            if (dbSuffixIdentifier != null)
                databaseName = string.Format("{0}_{1}", databaseName, dbSuffixIdentifier);

            var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
            modifyDatabaseDocument?.Invoke(doc);

            RavenOperationContext context;
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.Transaction = context.Environment.ReadTransaction();
                if (Server.ServerStore.Read(context, Constants.Database.Prefix + databaseName) != null)
                    throw new InvalidOperationException($"Database '{databaseName}' already exists");
            }

            var store = new DocumentStore
            {
                Url = UseFiddler(Server.Configuration.Core.ServerUrl),
                DefaultDatabase = databaseName,
            };
            ModifyStore(store);
            store.Initialize();

            await store.AsyncDatabaseCommands.GlobalAdmin.CreateDatabaseAsync(doc);
            store.AfterDispose += (sender, args) =>
            {
                store.AsyncDatabaseCommands.GlobalAdmin.DeleteDatabaseAsync(databaseName, hardDelete: true);
            };
            CreatedStores.Add(store);
          return store;
        }

        protected virtual void ModifyStore(DocumentStore store)
        {
            
        }

        private static string UseFiddler(string url)
        {
            if (Debugger.IsAttached && Process.GetProcessesByName("fiddler").Any())
                return url.Replace("localhost", "localhost.fiddler");

            return url;
        }

        public static void WaitForUserToContinueTheTest(DocumentStore documentStore, bool debug = true, int port = 8079)
        {
            if (debug && Debugger.IsAttached == false)
                return;

            string url = documentStore.Url;

            var databaseNameEncoded = Uri.EscapeDataString(documentStore.DefaultDatabase);
            var documentsPage = url + "/studio/index.html#databases/documents?&database=" + databaseNameEncoded + "&withStop=true";

            Process.Start(documentsPage); // start the server

            do
            {
                Thread.Sleep(100);
            } while (documentStore.DatabaseCommands.Head("Debug/Done") == null && (debug == false || Debugger.IsAttached));
        }


        public void Dispose()
        {
            foreach (var documentStore in CreatedStores)
            {
                documentStore.Dispose();
            }
            if (_localServer != null)
            {
                lock (ServerLocker)
                {
                    if (--_currentServerUsages > 0)
                        return;
                    _globalServer.Dispose();
                    _globalServer = null;
                    _currentServerUsages = 0;
                }
            }
        }
    }
}