using System;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_8700 : RavenTestBase
    {
        private class EntityGroupedByDayIndex : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                throw new NotImplementedException();
            }
        }

        [Fact]
        public void ThisWillCrashOnDocumentIdGenerator()
        {
            var documentStore = new DocumentStore
            {
                Urls = UseFiddler(Server.WebUrl),
                Database = "test-database"
            };
            // this seems to work, until .Store is called.
            Assert.Throws<InvalidOperationException>(() => documentStore.ExecuteIndex(new EntityGroupedByDayIndex()));

            Assert.Throws<InvalidOperationException>(() => documentStore.ExecuteIndexes(new[] {new EntityGroupedByDayIndex()}));
        }
    }
}
