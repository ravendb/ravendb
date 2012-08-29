using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Linq;
using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class WithStringReverse : RavenTest
	{
		[Fact]
		public void GivenSomeUsers_QueryWithAnIndex_ReturnsUsersWithNamesReversed()
		{
			using (EmbeddableDocumentStore store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("StringReverseIndex",
				                                new IndexDefinition
				                                	{
				                                		Map =
															"from doc in docs select new { doc.Name, ReverseName = doc.Name.Reverse())}"
				                                	});

				using (IDocumentSession documentSession = store.OpenSession())
				{
					documentSession.Store(new User {Name = "Ayende"});
					documentSession.Store(new User {Name = "Itamar"});
					documentSession.Store(new User {Name = "Pure Krome"});
					documentSession.Store(new User {Name = "John Skeet"});
					documentSession.Store(new User {Name = "StackOverflow"});
					documentSession.Store(new User {Name = "Wow"});
					documentSession.SaveChanges();
				}

				using (IDocumentSession documentSession = store.OpenSession())
				{
					var users = documentSession
						.Query<User>("StringReverseIndex")
						.Customize(x=>x.WaitForNonStaleResults())
						.ToList();

					Assert.Empty(store.DocumentDatabase.Statistics.Errors);
					Assert.NotNull(users);
					Assert.True(users.Count > 0);
				}
			}
		}

		public class ReversedResult
		{
			public string ReverseName { get; set; }
		}
		[Fact]
		public void CanQueryInReverse()
		{
			using (EmbeddableDocumentStore store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("StringReverseIndex",
												new IndexDefinition
												{
													Map =
														"from doc in docs select new { doc.Name, ReverseName = doc.Name.Reverse())}"
												});

				using (IDocumentSession documentSession = store.OpenSession())
				{
					documentSession.Store(new User { Name = "Ayende" });
					documentSession.Store(new User { Name = "Itamar" });
					documentSession.Store(new User { Name = "Pure Krome" });
					documentSession.Store(new User { Name = "John Skeet" });
					documentSession.Store(new User { Name = "StackOverflow" });
					documentSession.Store(new User { Name = "Wow" });
					documentSession.SaveChanges();
				}

				using (IDocumentSession documentSession = store.OpenSession())
				{
					var users = documentSession
						.Query<ReversedResult>("StringReverseIndex")
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x=>x.ReverseName.StartsWith("edn"))
						.As<User>()
						.ToList();

					Assert.Empty(store.DocumentDatabase.Statistics.Errors);
					Assert.True(users.Count > 0);
				}
			}
		}
	}
}