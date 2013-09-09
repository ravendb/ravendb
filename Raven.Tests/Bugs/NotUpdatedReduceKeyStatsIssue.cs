// -----------------------------------------------------------------------
//  <copyright file="NotUpdatedReduceKeyStatsIssue.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Bugs
{
	using System.Linq;
	using Client.Indexes;
	using Raven.Storage.Managed;
	using Xunit;

	public class NotUpdatedReduceKeyStatsIssue : RavenTest
	{
		public class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class UsersByName : AbstractIndexCreationTask<User, UsersByName.Result>
		{
			public class Result
			{
				public string Name { get; set; }
				public int Count { get; set; }
			}

			public UsersByName()
			{
				Map = users => from user in users
							   select new
									  {
										  user.Name,
										  Count = 1
									  };
				Reduce =
					results =>
					from result in results group result by result.Name into g select new { Name = g.Key, Count = g.Sum(x => x.Count) };
			}
		}

		[Fact]
		public void ShouldReturnCorrectResults()
		{
			using (var store = NewDocumentStore())
			{
				var index = new UsersByName();
				index.Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new User
					              {
									  Id = "Users/1",
						              Name = "Adam"
					              });

					session.Store(new User
								  {
									  Id = "Users/2",
									  Name = "Adam"
								  });

					session.Store(new User
								  {
									  Id = "Users/3",
									  Name = "John"
								  });

					session.SaveChanges();
				}

				var storage = store.DocumentDatabase.TransactionalStorage;

				using (var session = store.OpenSession())
				{
					session.Query<UsersByName.Result, UsersByName>().Customize(x => x.WaitForNonStaleResults()).ToList();
				}

                var indexId = store.DocumentDatabase.GetIndexDefinition(index.IndexName).IndexId;
				storage.Batch(accessor =>
				{
					var stats = accessor.MapReduce.GetKeysStats(indexId, 0, 10).ToList();
					Assert.Equal(2, stats.Count);
					Assert.Equal(2, stats.First(x => x.Key == "Adam").Count);
					Assert.Equal(1, stats.First(x => x.Key == "John").Count);
				});

				store.DocumentDatabase.Delete("Users/1", null, null); // delete "Adam" reduce key

				using (var session = store.OpenSession())
				{
					session.Query<UsersByName.Result, UsersByName>().Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				storage.Batch(accessor =>
				{
					var stats = accessor.MapReduce.GetKeysStats(indexId, 0, 10).ToList();
					Assert.Equal(2, stats.Count);
					Assert.Equal(1, stats.First(x => x.Key == "Adam").Count);
					Assert.Equal(1, stats.First(x => x.Key == "John").Count);
				});
			}
		}
	}
}