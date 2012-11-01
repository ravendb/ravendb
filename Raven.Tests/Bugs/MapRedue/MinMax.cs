using System;
using System.Linq;
using System.Threading;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs.MapRedue
{
	public class MinMax : RavenTest
	{
		private readonly EmbeddableDocumentStore store;

		private class User
		{
			public string Id { get; set; }
			public string UserName { get; set; }
		}

		public class LogInAction
		{
			public string Id { get; set; }
			public string UserId { get; set; }
			public bool WasSuccessful { get; set; }

			private DateTime? loggedInAt;
			public DateTime? LoggedInAt
			{
				get
				{
					if (loggedInAt == null && LoggedInAtWithOffset.HasValue)
					{
						loggedInAt = LoggedInAtWithOffset.Value.DateTime;
					}
					return loggedInAt;
				}
				set { loggedInAt = value; }
			}

			public DateTimeOffset? LoggedInAtWithOffset { get; set; }
		}

		public MinMax()
		{
			store = NewDocumentStore();
			using (var session = store.OpenSession())
			{
				var ayende = new User {UserName = "Ayende"};
				session.Store(ayende);

				session.Store(new LogInAction
				{
					UserId = ayende.Id,
					LoggedInAtWithOffset = DateTimeOffset.UtcNow.AddDays(-4),
					WasSuccessful = false,
				});
				session.Store(new LogInAction
				{
					UserId = ayende.Id,
					LoggedInAtWithOffset = DateTimeOffset.UtcNow.AddDays(-3),
					WasSuccessful = false,
				});
				session.Store(new LogInAction
				{
					UserId = ayende.Id,
					LoggedInAtWithOffset = DateTimeOffset.UtcNow.AddDays(-2),
					WasSuccessful = true,
				});

				session.SaveChanges();
			}
		}

		[Fact]
		public void CanUseMaxOnNullableDateTimeOffset()
		{
			new Users_LastLoggedInAt().Execute(store);

			using (var session = store.OpenSession())
			{
				var max = session.Query<Users_LastLoggedInAt.Result, Users_LastLoggedInAt>()
					.Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
					.ToList();

				WaitForUserToContinueTheTest(store);

				var databaseStatistics = store.DatabaseCommands.GetStatistics();
				Assert.Empty(databaseStatistics.Errors);

				Assert.NotEmpty(max);
			}
		}

		private class Users_LastLoggedInAt : AbstractMultiMapIndexCreationTask<Users_LastLoggedInAt.Result>
		{
			public class Result
			{
				public string UserName { get; set; }
				public DateTime? LoggedInAt { get; set; }
				public DateTimeOffset? LoggedInAtWithOffset { get; set; }
			}

			public Users_LastLoggedInAt()
			{
				AddMap<User>(users => users.Select(user => new Result
				{
					UserName = user.UserName,
					LoggedInAt = (DateTime?) null,
					LoggedInAtWithOffset = (DateTimeOffset?) null,
				}));

				AddMap<LogInAction>(actions => actions.Select(action => new Result
				{
					UserName = (string) null,
					LoggedInAt = action.LoggedInAt,
					LoggedInAtWithOffset = action.LoggedInAtWithOffset
				}));

				Reduce = results => from result in results
				                    group result by result.UserName
				                    into g
				                    select new Result
				                    {
					                    UserName = g.Key,
					                    LoggedInAt = g.Max(x => x.LoggedInAt),
					                    LoggedInAtWithOffset = g.Max(x => x.LoggedInAtWithOffset),
				                    };
			}
		}
	}
}