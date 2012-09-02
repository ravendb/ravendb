using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Indexing;
using Raven.Tests.Document;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Scott : RavenTest
	{
		[Fact]
		public void CanQueryMapReduceIndexGeo()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("TagCloud",
												new IndexDefinition
												{
													Map =
														@"
from post in docs.Posts 
from Tag in post.Tags
select new { Tag, Count = 1, Lat = 38.96939, Lon = -77.386398, _ = (object)null }",
													Reduce =
														@"
from result in results
group result by result.Tag into g
let lat = g.Select(x=>x.Lat).Where(x=>x!=null).FirstOrDefault()
let lng = g.Select(x=>x.Lon).Where(x=>x!=null).FirstOrDefault()
select new { 
	Tag = g.Key, 
	Count = g.Sum(x => (long)x.Count), 
	_ = SpatialIndex.Generate(lat,lng),
	Lat = lat, 
	Lon = lng }",
													Indexes = { { "Tag", FieldIndexing.NotAnalyzed } }
												});
				using (var session = store.OpenSession())
				{
					session.Store(new TagCloud.Post
					{
						PostedAt = SystemTime.UtcNow,
						Tags = new List<string> { "C#", "Programming", "NoSql" }
					});
					session.Store(new TagCloud.Post
					{
						PostedAt = SystemTime.UtcNow,
						Tags = new List<string> { "Database", "NoSql" }
					});
					session.SaveChanges();
					var tagAndCounts = session.Advanced.LuceneQuery<TagCloud.TagAndCount>("TagCloud")
						.WaitForNonStaleResults()
						.WithinRadiusOf(100, 38.96939, -77.386938)
						.WaitForNonStaleResults()
						.ToArray();
					Assert.Equal(1, tagAndCounts.First(x => x.Tag == "C#").Count);
					Assert.Equal(1, tagAndCounts.First(x => x.Tag == "Database").Count);
					Assert.Equal(2, tagAndCounts.First(x => x.Tag == "NoSql").Count);
					Assert.Equal(1, tagAndCounts.First(x => x.Tag == "Programming").Count);
				}
			}
		}
	}
}