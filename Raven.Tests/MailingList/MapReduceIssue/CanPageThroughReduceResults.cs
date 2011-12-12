using Raven.Client.Document;
using Raven.Client.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList.MapReduceIssue
{
	public class CanPageThroughReduceResults : RavenTest
	{
		[Fact]
		public void Test()
		{
			using(GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8080/"
			}.Initialize())
			{
				using (var stream = typeof(CanPageThroughReduceResults).Assembly.GetManifestResourceStream("Raven.Tests.MailingList.MapReduceIssue.MvcMusicStore_Dump.json"))
				{
					Smuggler.Smuggler.ImportData(stream, store.Url);
				}

				using (var session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					var artists = session.Query<Artist>("Artists")
						.Customize(x => x.WaitForNonStaleResults())
						.Statistics(out stats)
						.Take(4)
						.ToList();

					Assert.Equal(4, artists.Count);
					Assert.Equal(0, stats.SkippedResults);
					Assert.True(stats.TotalResults > 10);
				}


				using (var session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					var artists = session.Query<Artist>("Artists")
						.Customize(x => x.WaitForNonStaleResults())
						.Statistics(out stats)
						.Skip(3)
						.Take(4)
						.ToList();

					Assert.Equal(4, artists.Count);
					Assert.Equal(0, stats.SkippedResults);
					Assert.True(stats.TotalResults > 10);
				}

			}
		}

		public class Artist
		{
			public string Name { get; set; }
			public string Id { get; set; }
		}
	}
}