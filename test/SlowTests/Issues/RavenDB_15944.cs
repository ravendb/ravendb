using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15944 : RavenTestBase
    {
        public RavenDB_15944(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var databaseName = store.Database;

                await store.ExecuteIndexAsync(new TestDocumentsIndex(), databaseName);

                using (var session = store.OpenAsyncSession(databaseName))
                {
                    var testDoc1 = new TestDocument { Name = "Name1 (English)", Language = new CultureInfo("en-US") };
                    await session.StoreAsync(testDoc1);
                    var testDoc2 = new TestDocument { Name = "Name2 (French)", Language = new CultureInfo("fr-CA") };
                    await session.StoreAsync(testDoc2);

                    await session.SaveChangesAsync();
                }
                WaitForIndexing(store);
                using (var session = store.OpenAsyncSession(databaseName))
                {
                    var english = new CultureInfo("en-US");
                    var englishDocs = await session.Query<TestDocument, TestDocumentsIndex>()
                        .Where(x => x.Language.Equals(english)).ToListAsync();
                    Assert.Equal(1, englishDocs.Count);

                    var french = new CultureInfo("fr-CA");
                    var frenchDocs = await session.Query<TestDocument, TestDocumentsIndex>()
                        .Where(x => x.Language.Equals(french)).ToListAsync();
                    Assert.Equal(1, frenchDocs.Count);
                }
            }
        }

        public class TestDocument
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public CultureInfo Language { get; set; }
        }

        public class TestDocumentsIndex : AbstractIndexCreationTask<TestDocument>
        {
            public TestDocumentsIndex()
            {
                Map = docs =>
                    from doc in docs
                    select new
                    {
                        doc.Name,
                        doc.Language
                    };
            }
        }
    }
}
