using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5919 : RavenTestBase
    {
        [Fact]
        public void ChangingLockModeOrPriorityOnlyShouldNotResetIndex()
        {
            using (var store = GetDocumentStore())
            {
                var definition = new IndexDefinition
                {
                    Name = "Test",
                    Maps = { "from doc in docs select new { doc.Name }" }
                };

                var result1 = store.Admin.Send(new PutIndexesOperation(definition))[0];

                Assert.Equal(definition.Name, result1.Index);
                Assert.Equal(1, result1.IndexId);

                definition.LockMode = IndexLockMode.LockedError;
                definition.Priority = IndexPriority.High;

                var result2 = store.Admin.Send(new PutIndexesOperation(definition))[0];

                Assert.Equal(result1.IndexId, result2.IndexId);

                var serverDefinition = store.Admin.Send(new GetIndexOperation(definition.Name));
                Assert.Equal(serverDefinition.Priority, definition.Priority);
                Assert.Equal(serverDefinition.LockMode, definition.LockMode);
            }
        }
    }
}