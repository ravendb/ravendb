using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class IndexWhereClause : IDisposable
	{
		private IDocumentStore documentStore;
		private IDocumentSession documentSession;

		public IndexWhereClause()
		{
			documentStore = new EmbeddableDocumentStore
			{
				RunInMemory = true
			}.Initialize();
			documentSession = documentStore.OpenSession();
		}

		public void Dispose()
		{
			documentSession.Dispose();
			documentStore.Dispose();
		}

		[Fact]
		public void Where_clause_with_greater_than_or_less_than()
		{
			new MyIndex().Execute(documentStore);

			documentSession.Store(new Album() { Title = "RavenDB" });
			documentSession.Store(new Album() { Title = "RavenDB" });
			documentSession.Store(new Album() { Title = "RavenDB" });
			documentSession.Store(new Album() { Title = "RavenDB" });

			documentSession.SaveChanges();
			var albums = documentSession.Query<Album>().Customize(c => c.WaitForNonStaleResults()).ToList();
			Assert.Equal(albums.Count, 4);

			var result1 = documentSession.Query<MyIndex.ReduceResult,MyIndex>().Customize(c => c.WaitForNonStaleResults()).Where(i =>
i.Count == 4).ToList();
			Assert.Equal(result1.Count, 1);

			var result2 = documentSession.Query<MyIndex.ReduceResult,MyIndex>().Customize(c => c.WaitForNonStaleResults()).Where(i =>
i.Count > 1).ToList();
			Assert.Equal(result2.Count, 1);

			var result3 = documentSession.Query<MyIndex.ReduceResult,MyIndex>().Customize(c => c.WaitForNonStaleResults()).Where(i =>
i.Count < 5).ToList();
			Assert.Equal(result3.Count, 1);
		}

		public class Album
		{
			public int Id { get; set; }
			public string Title { get; set; }
			public decimal Price { get; set; }


		}

		public class MyIndex : AbstractIndexCreationTask<Album, MyIndex.ReduceResult>
		{
			public class ReduceResult
			{
				public string Title { get; set; }
				public int Count { get; set; }
			}

			public MyIndex()
			{
				this.Map = albums => from album in albums
									 select new ReduceResult { Title = album.Title, Count = 1 };
				this.Reduce = results => from r in results
										 group r by r.Title into
											 g
										 select new ReduceResult
										 {
											 Title = g.Key,
											 Count = g.Sum(x =>
												 x.Count)
										 };
			}
		}
	}
}