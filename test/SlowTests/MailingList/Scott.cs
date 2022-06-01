using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Util;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Scott : RavenTestBase
    {
        public Scott(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanQueryMapReduceIndexGeo(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "TagCloud",
                    Maps = {
                        @"
from post in docs.Posts 
from Tag in post.Tags
select new { Tag, Count = 1, Lat = 38.96939, Lon = -77.386398, Coordinates = (object)null }"
                    },
                    Reduce =
                        @"
from result in results
group result by result.Tag into g
let lat = g.Select(x=>x.Lat).Where(x=>x!=null).FirstOrDefault()
let lng = g.Select(x=>x.Lon).Where(x=>x!=null).FirstOrDefault()
select new { 
    Tag = g.Key, 
    Count = g.Sum(x => (long)x.Count), 
    Coordinates = CreateSpatialField(lat,lng),
    Lat = lat, 
    Lon = lng }",
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                                                        { "Tag", new IndexFieldOptions {Indexing = FieldIndexing.Exact}}
                    }
                }));
                using (var session = store.OpenSession())
                {
                    session.Store(new Post
                    {
                        PostedAt = SystemTime.UtcNow,
                        Tags = new List<string> { "C#", "Programming", "NoSql" }
                    });
                    session.Store(new Post
                    {
                        PostedAt = SystemTime.UtcNow,
                        Tags = new List<string> { "Database", "NoSql" }
                    });
                    session.SaveChanges();
                    var tagAndCounts = session.Advanced.DocumentQuery<TagAndCount>("TagCloud")
                        .WaitForNonStaleResults()
                        .WithinRadiusOf("Coordinates", 100, 38.96939, -77.386938)
                        .WaitForNonStaleResults()
                        .ToArray();
                    Assert.Equal(1, tagAndCounts.First(x => x.Tag == "C#").Count);
                    Assert.Equal(1, tagAndCounts.First(x => x.Tag == "Database").Count);
                    Assert.Equal(2, tagAndCounts.First(x => x.Tag == "NoSql").Count);
                    Assert.Equal(1, tagAndCounts.First(x => x.Tag == "Programming").Count);
                }
            }
        }

        private class Post
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public DateTime PostedAt { get; set; }
            public List<string> Tags { get; set; }

            public string Content { get; set; }
        }

        private class TagAndCount
        {
            public string Tag { get; set; }
            public long Count { get; set; }

            public override string ToString()
            {
                return string.Format("Tag: {0}, Count: {1}", Tag, Count);
            }
        }
    }
}
