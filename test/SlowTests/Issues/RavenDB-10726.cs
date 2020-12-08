using System;
using FastTests;
using Raven.Client.Documents.Operations.Attachments;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10726 : RavenTestBase
    {
        public RavenDB_10726(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Operations_CanBeFetchedForGivenDatabase_WithoutDefaultDatabaseSet()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore =>
                {
                    documentStore.Database = null;
                }
            }))
            {
                Assert.Null(store.Database);
                store.Operations.ForDatabase("some-db");
            }
        }

        [Fact]
        public void MaintenanceOperations_CanBeFetchedForGivenDatabase_WithoutDefaultDatabaseSet()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore =>
                {
                    documentStore.Database = null;
                }
            }))
            {
                Assert.Null(store.Database);
                store.Maintenance.ForDatabase("some-db");
            }
        }

        [Fact]
        public void ShouldThrow_WhenTryingToChangeDefaultDbName_AfterInitialization()
        {
            using (var store = GetDocumentStore())
            {
                var t = Assert.Throws<InvalidOperationException>(() => store.Database = null);
                Assert.Equal("You cannot set 'Database' after the document store has been initialized.", t.Message);
            }
        }

        [Fact]
        public void ShouldThrow_IfTryingToUseOperationsDirectly_WithoutDefaultDatabaseSet()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore =>
                {
                    documentStore.Database = null;
                }
            }))
            {
                Assert.Null(store.Database);

                var t = Assert.Throws<InvalidOperationException>(() => store.Operations.Send(new DeleteAttachmentOperation("orders/1-A", "001.jpg")));
                Assert.Equal("Cannot use Operations without a database defined, did you forget to call ForDatabase?", t.Message);

                t = Assert.Throws<InvalidOperationException>(() => store.Maintenance.Send(new CreateSampleDataOperation()));
                Assert.Equal("Cannot use Maintenance without a database defined, did you forget to call ForDatabase?", t.Message);
            }
        }

        [Fact]
        public void ShouldThrow_IfTryingToUse_AggressiveCaching_OrSetRequestTimeout_WithoutDefaultDatabaseSet_AndNoDatabaseParameter()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore =>
                {
                    documentStore.Database = null;
                }
            }))
            {
                Assert.Null(store.Database);

                var t = Assert.Throws<InvalidOperationException>(() => store.AggressivelyCache());
                Assert.Contains("Cannot determine database to operate on.", t.Message);

                t = Assert.Throws<InvalidOperationException>(() => store.AggressivelyCacheFor(TimeSpan.FromMinutes(1)));
                Assert.Contains("Cannot determine database to operate on.", t.Message);

                t = Assert.Throws<InvalidOperationException>(() => store.DisableAggressiveCaching());
                Assert.Contains("Cannot determine database to operate on.", t.Message);

                t = Assert.Throws<InvalidOperationException>(() => store.SetRequestTimeout(TimeSpan.FromSeconds(10)));
                Assert.Contains("Cannot determine database to operate on.", t.Message);
            }
        }
    }
}
