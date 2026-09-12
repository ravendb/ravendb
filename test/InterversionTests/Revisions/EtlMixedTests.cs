using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using static InterversionTests.Revisions.RevisionsInterversionHelpers;

namespace InterversionTests.Revisions
{
    public class EtlMixedTests : InterversionTestBase
    {
        public EtlMixedTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Etl | RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task EtlNewToOld_DocumentsAndRevisions_Converge()
        {
            var customSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false",
                [RavenConfiguration.GetKey(x => x.Licensing.EulaAccepted)] = "true",
            };

            var oldNode = await GetServerAsync(Versions.PrePRv62, customSettings: customSettings);

            using var srcStore = GetDocumentStore(new Options
            {
                Path = NewDataPath(suffix: "src"),
                RunInMemory = false
            });

            var destDb = GetDatabaseName() + "-etldest";
            using var destStore = new DocumentStore
            {
                Urls = new[] { oldNode.Url },
                Database = destDb
            };
            destStore.Initialize();
            await destStore.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(destDb)
            {
                Settings = { [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false" }
            }));

            await ConfigureRevisionsAsync(srcStore);
            await ConfigureRevisionsAsync(destStore);

            Etl.AddEtl(srcStore, destStore, "Users", script: @"loadToUsers(this);");

            var etlDone = Etl.WaitForEtlToComplete(srcStore);

            using (var session = srcStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Joe" }, "users/1");
                await session.SaveChangesAsync();
            }
            using (var session = srcStore.OpenAsyncSession())
            {
                var u = await session.LoadAsync<User>("users/1");
                u.Name = "Joe Doe";
                await session.SaveChangesAsync();
            }

            Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

            await WaitForDocumentNameAsync(destStore, "users/1", expectedName: "Joe Doe");
        }
    }
}
