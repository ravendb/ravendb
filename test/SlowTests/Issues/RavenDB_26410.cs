using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Config;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_26410 : RavenTestBase
    {
        public RavenDB_26410(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task RavenEtl_uses_database_default_load_request_timeout_when_not_specified()
        {
            using (var src = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                    record.Settings[RavenConfiguration.GetKey(x => x.Etl.RavenLoadRequestTimeout)] = "33"
            }))
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, new[] { "Users" }, script: "loadToUsers(this);", out var configuration);

                var etlProcess = await GetRavenEtlProcess(src, configuration.Name);
                Assert.Null(etlProcess.Configuration.LoadRequestTimeoutInSec);

                var database = await GetDocumentDatabaseInstanceFor(src);
                Assert.Equal(TimeSpan.FromSeconds(33), database.Configuration.Etl.RavenLoadRequestTimeout.AsTimeSpan);

                TimeSpan? actualTimeout = null;
                var afterRun = new AsyncManualResetEvent();

                etlProcess.BeforeActualLoad += (_, batchCommand) =>
                {
                    actualTimeout = batchCommand.Timeout;
                    afterRun.Set();
                };

                using (var session = src.OpenSession())
                {
                    session.Store(new User { Name = "Test" });
                    session.SaveChanges();
                }

                await afterRun.WaitAsync(TimeSpan.FromSeconds(30));

                Assert.Equal(TimeSpan.FromSeconds(33), actualTimeout);
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task RavenEtl_explicit_load_request_timeout_overrides_database_default()
        {
            using (var src = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                    record.Settings[RavenConfiguration.GetKey(x => x.Etl.RavenLoadRequestTimeout)] = "33"
            }))
            using (var dest = GetDocumentStore())
            {
                var addResult = Etl.AddEtl(src, dest, new[] { "Users" }, script: "loadToUsers(this);", out var configuration);
                configuration.LoadRequestTimeoutInSec = 17;

                var updateResult = src.Maintenance.Send(new UpdateEtlOperation<RavenConnectionString>(addResult.TaskId, configuration));

                await Databases.WaitForRaftIndex(src.Database, updateResult.RaftCommandIndex);

                var database = await GetDocumentDatabaseInstanceFor(src);

                // wait to process to get updated configuration

                var timeoutValue = await WaitForValueAsync(async () =>
                {
                    var process = await GetRavenEtlProcess(src, configuration.Name);
                    
                    return process?.Configuration.LoadRequestTimeoutInSec;
                }, 17, interval: 333);

                Assert.Equal(17, timeoutValue);

                var etlProcess = await GetRavenEtlProcess(src, configuration.Name);
                
                Assert.Equal(17, etlProcess.Configuration.LoadRequestTimeoutInSec);

                Assert.Equal(TimeSpan.FromSeconds(33), database.Configuration.Etl.RavenLoadRequestTimeout.AsTimeSpan);

                TimeSpan? actualTimeout = null;
                var afterRun = new AsyncManualResetEvent();

                etlProcess.BeforeActualLoad += (_, batchCommand) =>
                {
                    actualTimeout = batchCommand.Timeout;
                    afterRun.Set();
                };

                using (var session = src.OpenSession())
                {
                    session.Store(new User { Name = "Test" });
                    session.SaveChanges();
                }

                Assert.True(await afterRun.WaitAsync(TimeSpan.FromSeconds(30)));

                Assert.Equal(TimeSpan.FromSeconds(17), actualTimeout);
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task RavenEtl_uses_global_default_timeout_when_nothing_is_configured()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, new[] { "Users" }, script: "loadToUsers(this);", out var configuration);

                Assert.Null(configuration.LoadRequestTimeoutInSec);

                var etlProcess = await GetRavenEtlProcess(src, configuration.Name);
                Assert.Null(etlProcess.Configuration.LoadRequestTimeoutInSec);

                var database = await GetDocumentDatabaseInstanceFor(src);
                Assert.Equal(TimeSpan.FromSeconds(300), database.Configuration.Etl.RavenLoadRequestTimeout.AsTimeSpan);

                TimeSpan? actualTimeout = null;
                var afterRun = new AsyncManualResetEvent();

                etlProcess.BeforeActualLoad += (_, batchCommand) =>
                {
                    actualTimeout = batchCommand.Timeout;
                    afterRun.Set();
                };

                using (var session = src.OpenSession())
                {
                    session.Store(new User { Name = "Test" });
                    session.SaveChanges();
                }

                Assert.True(await afterRun.WaitAsync(TimeSpan.FromSeconds(30)));

                Assert.Equal(TimeSpan.FromSeconds(300), actualTimeout);
            }
        }

        private async Task<RavenEtl> GetRavenEtlProcess(DocumentStore store, string configurationName)
        {
            var database = await GetDocumentDatabaseInstanceFor(store);
            return Assert.IsType<RavenEtl>(Assert.Single(database.EtlLoader.Processes, x => x.ConfigurationName == configurationName));
        }
    }
}
