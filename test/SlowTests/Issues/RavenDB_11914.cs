using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11914 : RavenTestBase
    {
        public RavenDB_11914(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanSendOperationsWithSettingExplicitDbName(Options options)
        {
            using (var store = GetDocumentStore(options))
            using (var other = new DocumentStore
            {
                Urls = store.Urls,
            }.Initialize())
            {
                other.Operations.ForDatabase(store.Database).Send(
                    new DeleteByQueryOperation<User>("test", x => x.Name == "arek"));
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanSendOperationsWithSettingExplicitDbName2(Options options)
        {
            using (var store = GetDocumentStore(options))
            using (var other = new DocumentStore
            {
                Urls = store.Urls,
                Database = string.Empty
            }.Initialize())
            {
                other.Operations.ForDatabase(store.Database).Send(
                    new DeleteByQueryOperation<User>("test", x => x.Name == "arek"));
            }
        }
    }
}
