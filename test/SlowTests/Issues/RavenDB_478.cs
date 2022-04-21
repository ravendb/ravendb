using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_478 : RavenTestBase
    {
        public RavenDB_478(ITestOutputHelper output) : base(output)
        {
        }

        private class ProductContent
        {
            public string Slug { get; set; }
            public string Title { get; set; }
            public string Content { get; set; }
        }
        private class Product
        {
            public string ShortName { get; set; }
            public string Name { get; set; }
            public string Summary { get; set; }
            public string Description { get; set; }
            public string HomepageContent { get; set; }
        }

        private class ContentSearchIndex : AbstractMultiMapIndexCreationTask<ContentSearchIndex.Result>
        {
            public class Result
            {
                public string Slug { get; set; }
                public string Title { get; set; }
                public string Content { get; set; }
            }

            public ContentSearchIndex()
            {
                AddMap<ProductContent>(docs => from content in docs
                                               select new { Slug = content.Slug.Boost(15), Title = content.Title.Boost(13), content.Content });

                AddMap<Product>(docs => from product in docs
                                        select
                                            new
                                            {
                                                Slug = product.ShortName.Boost(30),
                                                Title = product.Name.Boost(25),
                                                Content =
                                            new string[] { product.Summary, product.Description, product.HomepageContent }.Boost(20)
                                            });


                Index(x => x.Slug, FieldIndexing.Search);
                Index(x => x.Title, FieldIndexing.Search);
                Index(x => x.Content, FieldIndexing.Search);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void IndexWithBoost(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new ContentSearchIndex().Execute(store);
            }
        }
    }
}
