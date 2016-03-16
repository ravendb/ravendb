using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Xunit;

namespace Raven.Tests.Core
{
    public class RavenTestBase : IDisposable
    {
        public const string ServerName = "Raven.Tests.Core.Server";

        protected readonly List<DocumentStore> CreatedStores = new List<DocumentStore>();
        protected static readonly HashSet<string> PathsToDelete = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private static long _currentServerUsages;
        private static RavenServer _globalServer;
        private static readonly object ServerLocker = new object();
        private RavenServer _localServer;
        private static int _pathCount;

        public RavenServer Server
        {
            get
            {
                if (_localServer != null)
                    return _localServer;
                if (_globalServer != null)
                {
                    Interlocked.Increment(ref _currentServerUsages);
                    _localServer = _globalServer;
                    return _localServer;
                }
                lock (ServerLocker)
                {
                    if (_globalServer == null)
                        _globalServer = CreateServer();
                    Interlocked.Increment(ref _currentServerUsages);
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

            TransactionOperationContext context;
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
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

            await store.AsyncDatabaseCommands.GlobalAdmin.CreateDatabaseAsync(doc).ConfigureAwait(false);
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

        protected string NewDataPath(string prefix = null, bool forceCreateDir = false)
        {
            prefix = prefix?.Replace("<", "").Replace(">", "");

            var newDataDir = Path.GetFullPath(string.Format(@".\{1}-{0}-{2}\", DateTime.Now.ToString("yyyy-MM-dd,HH-mm-ss"), prefix ?? "TestDatabase", Interlocked.Increment(ref _pathCount)));
            if (forceCreateDir && Directory.Exists(newDataDir) == false)
                Directory.CreateDirectory(newDataDir);
            PathsToDelete.Add(newDataDir);
            return newDataDir;
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);

            var errors = new List<Exception>();
            foreach (var store in CreatedStores)
            {
                try
                {
                    store.Dispose();
                }
                catch (Exception e)
                {
                    errors.Add(e);
                }
            }

            if (_localServer != null)
            {
                if(Interlocked.Decrement(ref _currentServerUsages)>0)
                    return;
                lock (ServerLocker)
                {
                    try
                    {
                        _globalServer.Dispose();
                        _globalServer = null;
                        _currentServerUsages = 0;
                    }
                    catch (Exception e)
                    {
                        errors.Add(e);
                    }
                }
            }

            GC.Collect(2);
            GC.WaitForPendingFinalizers();

            foreach (var pathToDelete in PathsToDelete)
            {
                try
                {
                    ClearDatabaseDirectory(pathToDelete);
                }
                catch (Exception e)
                {
                    errors.Add(e);
                }
                finally
                {
                    if (File.Exists(pathToDelete)) // Just in order to be sure we didn't created a file in that path, by mistake)
                    {
                        errors.Add(new IOException(string.Format("We tried to delete the '{0}' directory, but failed because it is a file.\r\n{1}", pathToDelete,
                            WhoIsLocking.ThisFile(pathToDelete))));
                    }
                    else if (Directory.Exists(pathToDelete))
                    {
                        string filePath;
                        try
                        {
                            filePath = Directory.GetFiles(pathToDelete, "*", SearchOption.AllDirectories).FirstOrDefault() ?? pathToDelete;
                        }
                        catch (Exception)
                        {
                            filePath = pathToDelete;
                        }
                        errors.Add(new IOException(string.Format("We tried to delete the '{0}' directory.\r\n{1}", pathToDelete,
                            WhoIsLocking.ThisFile(filePath))));
                    }
                }
            }

            if (errors.Count > 0)
                throw new AggregateException(errors);
        }

        private void ClearDatabaseDirectory(string dataDir)
        {
            var isRetry = false;

            while (true)
            {
                try
                {
                    IOExtensions.DeleteDirectory(dataDir);
                    break;
                }
                catch (IOException)
                {
                    if (isRetry)
                        throw;

                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    isRetry = true;

                    Thread.Sleep(2500);
                }
            }
        }


        protected static void LowLevel_WaitForIndexMap(Index index, long etag)
        {
            var timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(15);
            Assert.True(SpinWait.SpinUntil(() => index.GetLastMappedEtagsForDebug().Values.Min() == etag, timeout));
        }

        protected DocumentDatabase LowLevel_CreateDocumentDatabase([CallerMemberName] string caller = null, bool runInMemory = true, string dataDirectory = null)
        {
            var name = caller ?? Guid.NewGuid().ToString("N");

            if (string.IsNullOrEmpty(dataDirectory) == false)
                PathsToDelete.Add(dataDirectory);
            else
                dataDirectory = NewDataPath(name);

            var configuration = new RavenConfiguration();
            configuration.Initialize();
            configuration.Core.RunInMemory = runInMemory;
            configuration.Core.DataDirectory = dataDirectory;

            var documentDatabase = new DocumentDatabase(name, configuration);
            documentDatabase.Initialize();

            return documentDatabase;
        }

    }
}