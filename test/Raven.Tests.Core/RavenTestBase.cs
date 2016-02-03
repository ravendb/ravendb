using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
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

        public static RavenServer Server { get; }

        static RavenTestBase()
        {
            var configuration = new RavenConfiguration();
            configuration.Initialize();

            configuration.Core.ServerUrls = new[] { "http://localhost:8089" };
            configuration.Server.Name = ServerName;
            configuration.Core.RunInMemory = true;
            configuration.Core.DataDirectory = Path.Combine(configuration.Core.DataDirectory, "Tests");
            configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(10, TimeUnit.Seconds);
            configuration.Storage.AllowOn32Bits = true;

            IOExtensions.DeleteDirectory(configuration.Core.DataDirectory);

            // NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(Port);

            Server = new RavenServer(configuration)
            {
            };
            Server.Initialize();
        }

        protected virtual async Task<DocumentStore> GetDocumentStore([CallerMemberName] string databaseName = null, string dbSuffixIdentifier = null,
           Action<DatabaseDocument> modifyDatabaseDocument = null)
        {
            if (dbSuffixIdentifier != null)
                databaseName = string.Format("{0}_{1}", databaseName, dbSuffixIdentifier);

            var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
            if (modifyDatabaseDocument != null)
                modifyDatabaseDocument(doc);

            RavenOperationContext context;
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.Transaction = context.Environment.ReadTransaction();
                if (Server.ServerStore.Read(context, Constants.Database.Prefix + databaseName) != null)
                    throw new InvalidOperationException($"Database '{databaseName}' already exists");
            }
            
            var store = new DocumentStore
            {
                Url = UseFiddler(Server.Configuration.Core.ServerUrls.First()),
                DefaultDatabase = databaseName,
            };
            store.Initialize();

            do
            {
                Thread.Sleep(100);
            } while (true);

            await store.AsyncDatabaseCommands.GlobalAdmin.CreateDatabaseAsync(doc);

            CreatedStores.Add(store);
            return store;
        }

        private string UseFiddler(string url)
        {
            return url.Replace("localhost", "localhost.fiddler");
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
            // TODO: Delete database here
        }
    }
}