using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10859 : RavenTestBase
    {
        public RavenDB_10859(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldCompile()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "test",
                    Maps =
                    {
                        @"from p in docs.ProductViews
select new {
MemberId = p.MemberId,
TagName = LoadDocument(p.ProductId,""Products"").Tags.Where(x=>x.Confidence > 90).OrderByDescending(x=>x.Confidence).Select(x=>x.Name).FirstOrDefault(),
                        TagCount = 1
                    }"
                        },
                    Reduce = @"
                    from result in results
                    group result by new{
                    result.MemberId,result.TagName   
                }into g
                select new {
                    MemberId = g.Key.MemberId, 
                    TagName = g.Key.TagName, 
                    TagCount = g.Sum(x=>x.TagCount)
                }"
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new ProductView
                    {
                        Id = "productViews/1",
                        MemberId = "member1",
                        ProductId = "products/1"
                    });

                    session.Store(new Product
                    {
                        Id = "products/1",
                        Tags = new List<Tag>
                        {
                            new Tag
                            {
                                Name = "tagName1",
                                Confidence = 100
                            }
                        }
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation("test"));
                Assert.Equal(0, indexStats.ErrorsCount);

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Advanced
                        .RawQuery<dynamic>("from index 'test'")
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("member1", results[0].MemberId.ToString());
                    Assert.Equal("tagName1", results[0].TagName.ToString());
                    Assert.Equal(1, (int)results[0].TagCount);
                }
            }
        }

        private class Tag
        {
            public string Name { get; set; }
            public int Confidence { get; set; }
        }

        private class Product
        {
            public string Id { get; set; }
            public List<Tag> Tags { get; set; }
        }

        private class ProductView
        {
            public string Id { get; set; }
            public string MemberId { get; set; }
            public string ProductId { get; set; }
        }
    }
}
