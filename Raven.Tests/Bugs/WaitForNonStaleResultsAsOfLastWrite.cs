using System.Threading;
using Raven.Client.Indexes;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class WaitForNonStaleResultsAsOfLastWrite : LocalClientTest
	{
		[Fact]
		public void WillRecordLastWrittenEtag()
		{
			using (var store = NewDocumentStore())
			{
				Assert.Null(store.GetLastWrittenEtag());


				using(var session = store.OpenSession())
				{
					session.Store(new User());
					session.SaveChanges();
				}

				Assert.NotNull(store.GetLastWrittenEtag());
			}
		}


		[Fact]
		public void WillChangeEtagsAfterSecondWrite()
		{
			using (var store = NewDocumentStore())
			{
				Assert.Null(store.GetLastWrittenEtag());


				using (var session = store.OpenSession())
				{
					session.Store(new User());
					session.SaveChanges();
				}

				var firstWrittenEtag = store.GetLastWrittenEtag();
				using (var session = store.OpenSession())
				{
					session.Store(new User());
					session.SaveChanges();
				}
				Assert.NotEqual(firstWrittenEtag, store.GetLastWrittenEtag());
			}
		}


		[Fact]
		public void CanExpclitlyAskForNonStaleAsOfLastWrite()
		{
			using(var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User());
					session.SaveChanges();

					var users = session.Query<object, RavenDocumentsByEntityName>()
						.Customize(x=>x.WaitForNonStaleResultsAsOfLastWrite())
						.ToList();

					Assert.NotEmpty(users);
				}

			}
		}

		[Fact]
		public void WillIgnoreUnIndexedChangesLaterInTheGame()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User());
					session.SaveChanges();

					// this is where we record the etag value
					var usersQuery = session.Advanced.LuceneQuery<object,RavenDocumentsByEntityName>()
						.WaitForNonStaleResultsAsOfLastWrite();

					// wait for indexing to complete
					while(store.DocumentDatabase.Statistics.StaleIndexes.Length > 0)
						Thread.Sleep(100);

					store.DocumentDatabase.StopBackgroundWorkers();

					session.Store(new User());
					session.SaveChanges();

					var objects = usersQuery.ToList();

					Assert.Equal(1, objects.Count);
				}

			}
		}
	}
}