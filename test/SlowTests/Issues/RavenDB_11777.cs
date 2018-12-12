using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11777 : RavenTestBase
    {
        private class Image
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public ICollection<string> Users { get; set; }
            public ICollection<string> Tags { get; set; }
        }

        private class ImageByName : AbstractIndexCreationTask<Image, ImageByName.ReduceResult>
        {
            public class ReduceResult
            {
                public string Id { get; set; }
                public string Name { get; set; }
            }

            public ImageByName()
            {
                Map = docs => from i in docs
                              select new
                              {
                                  Id = i.Id,
                                  Name = new[] { i.Name },
                              };
                Index(r => r.Name, FieldIndexing.Search);
                Analyzers.Add(n => n.Name, nameof(NGramAnalyzer));
            }
        }

        [Fact]
        public async Task CanChangeNGramConfiguration()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDocumentDatabaseInstanceFor(store);

                Assert.Equal(2, database.Configuration.Indexing.MinGram);
                Assert.Equal(6, database.Configuration.Indexing.MaxGram);

                var definition = new ImageByName().CreateIndexDefinition();
                definition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MinGram)] = "5";
                definition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MaxGram)] = "8";

                await store.Maintenance.SendAsync(new PutIndexesOperation(definition));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Image { Id = "1", Name = "Great Photo buddy" });
                    await session.StoreAsync(new Image { Id = "2", Name = "Nice Photo of the sky" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var images = await session.Query<Image, ImageByName>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderBy(x => x.Name)
                        .Search(x => x.Name, "photo")
                        .ToListAsync();

                    Assert.NotEmpty(images);
                }
            }
        }
    }
}
