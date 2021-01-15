using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server;
using Raven.Server.Config;
using Xunit.Abstractions;

namespace BenchmarkTests
{
    public abstract class BenchmarkTestBase : RavenTestBase
    {
        protected BenchmarkTestBase(ITestOutputHelper output) : base(output)
        {
        }

        public abstract Task InitAsync(DocumentStore store);

        protected override RavenServer GetNewServer(ServerCreationOptions options = null, [CallerMemberName]
            string caller = null)
        {
            var customSettings = new Dictionary<string, string>
            {
                {RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime), int.MaxValue.ToString()}
            };
            
            if (Encrypted)
            {
                var serverUrl = UseFiddlerUrl("https://127.0.0.1:0");
                SetupServerAuthentication(customSettings, serverUrl);
            }
            else
            {
                var serverUrl = UseFiddlerUrl("http://127.0.0.1:0");
                customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl;
            }

            var co = new ServerCreationOptions
            {
                CustomSettings = customSettings,
                RunInMemory = false, 
                RegisterForDisposal = false,
                NodeTag = "A",
                DataDirectory = RavenTestHelper.NewDataPath("benchmark", 0, true)
            };
            var server = base.GetNewServer(co, caller);
            Servers.Add(server);

            return server;
        }

        protected bool Encrypted
        {
            get
            {
                return bool.TryParse(Environment.GetEnvironmentVariable("TEST_FORCE_ENCRYPTED_STORAGE"), out var value) && value;
            }
        }

        protected DocumentStore GetSimpleDocumentStore(string databaseName, bool deleteDatabaseOnDispose = true)
        {
            X509Certificate2 adminCert = null;
            
            if (Encrypted)
            {
                var certificates = GenerateAndSaveSelfSignedCertificate();
                adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value,
                    certificates.ClientCertificate1.Value,
                    new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin,
                    Server);
            }
            

            var store = new DocumentStore
            {
                Urls = new[]
                {
                    Server.WebUrl
                }, 
                Database = databaseName, 
                Certificate = adminCert
            };

            if (deleteDatabaseOnDispose)
            {
                store.BeforeDispose += (sender, args) =>
                {
                    try
                    {
                        store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true));
                    }
                    catch (Exception)
                    {
                        // ignored
                    }
                };
            }

            store.Initialize();

            return store;
        }

        protected override DocumentStore GetDocumentStore(Options options = null, [CallerMemberName]string caller = null)
        {
            // since we want server to survive between tests runs 
            // we have to cheat a little bit
            // benchmark tests are divided into 2 phases: 
            // 1. initialization 
            // 2. actual test execution (this part is measured)
            
            var server = Server; 

            if (Servers.Contains(server) == false)
            {
                Servers.Add(server);
            }

            if (options == null)
                options = new Options();

            options.Encrypted = Encrypted && options.CreateDatabase; // don't set encryption if we don't create new db
            options.ModifyDatabaseRecord = record => record.Settings.Remove(RavenConfiguration.GetKey(x => x.Core.RunInMemory));

            return base.GetDocumentStore(options, caller);
        }

        protected async Task WaitForIndexAsync(DocumentStore store, string databaseName, string indexName, TimeSpan? timeout = null)
        {
            if (timeout == null)
                timeout = TimeSpan.FromMinutes(10);

            var admin = store.Maintenance.ForDatabase(databaseName);

            var sp = Stopwatch.StartNew();
            while (sp.Elapsed < timeout.Value)
            {
                var indexStats = await admin.SendAsync(new GetIndexStatisticsOperation(indexName));
                if (indexStats == null)
                    IndexDoesNotExistException.ThrowFor(indexName);

                if (indexStats.Status != IndexRunningStatus.Running)
                    throw new InvalidOperationException($"Index '{indexName}' is not running!");

                if (indexStats.State != IndexState.Idle && indexStats.State != IndexState.Normal)
                    throw new InvalidOperationException($"Index '{indexName}' state ({indexStats.State}) is invalid!");

                if (indexStats.IsStale == false)
                    return;

                await Task.Delay(32);
            }

            throw new TimeoutException($"The index '{indexName}' stayed stale for more than {timeout.Value}.");
        }

        protected DatabaseRecord CreateDatabaseRecord(string databaseName)
        {
            var databaseRecord = new DatabaseRecord(databaseName);
            
            if (Encrypted)
            {
                PutSecrectKeyForDatabaseInServersStore(databaseName, Server);
                databaseRecord.Encrypted = true;
            }

            return databaseRecord;
        }
    }
}
