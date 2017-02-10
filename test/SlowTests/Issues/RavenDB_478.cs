using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Raven.Client.Indexing;
using Raven.Client.Linq.Indexing;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_478 : RavenNewTestBase
    {
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


                Index(x => x.Slug, FieldIndexing.Analyzed);
                Index(x => x.Title, FieldIndexing.Analyzed);
                Index(x => x.Content, FieldIndexing.Analyzed);
            }
        }

        [Fact]
        public void IndexWithBoost()
        {
            using (var store = GetDocumentStore())
            {
                new ContentSearchIndex().Execute(store);
            }
        }
    }
}
