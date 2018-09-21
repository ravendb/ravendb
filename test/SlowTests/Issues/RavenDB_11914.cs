using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11914 : RavenTestBase
    {
        [Fact]
        public void CanSendOperationsWithSettingExplicitDbName()
        {
            using (var store = GetDocumentStore())
            using (var other = new DocumentStore
            {
                Urls = store.Urls,
            }.Initialize())
            {
                other.Operations.ForDatabase(store.Database).Send(
                    new DeleteByQueryOperation<User>("test", x => x.Name == "arek"));
            }
        }

        [Fact]
        public void CanSendOperationsWithSettingExplicitDbName2()
        {
            using (var store = GetDocumentStore())
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
