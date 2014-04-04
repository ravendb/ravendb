using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.Issues
{
	public class RavenDB_72 : RavenTest
	{
		[Fact]
		public void CanWork()
		{
			using (var store = NewDocumentStore())
			{
				const string searchQuery = "Doe";

				// Scan for all indexes inside the ASSEMBLY.
				new Users_ByDisplayNameReversed().Execute(store);

				// Seed some fake data.
				CreateFakeData(store);

				var xx = new string(searchQuery.Reverse().ToArray());

				// Now lets do our query.
				using (IDocumentSession documentSession = store.OpenSession())
				{

					var users = documentSession
						.Query<Users_ByDisplayNameReversed.Result, Users_ByDisplayNameReversed>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.DisplayNameReversed.StartsWith(xx))
						.As<User>()
						.ToList();

					Assert.NotEmpty(users);
				}


				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
			}
		}

		public class User
		{
			public string Id { get; set; }
			public string DisplayName { get; set; }
		}

		public class Users_ByDisplayNameReversed : AbstractIndexCreationTask<User, Users_ByDisplayNameReversed.Result>
		{
			public Users_ByDisplayNameReversed()
			{
				Map = docs => from doc in docs
							  select new
							  {
								  doc.Id,
								  doc.DisplayName,
								  DisplayNameReversed = doc.DisplayName.Reverse(),
							  };

				//Index(x => x.DisplayNameReversed, FieldIndexing.NotAnalyzed);
			}

			public class Result
			{
				public string Id { get; set; }
				public string DisplayName { get; set; }
				public string DisplayNameReversed { get; set; }
			}
		}

		private static void CreateFakeData(IDocumentStore documentStore)
		{
			if (documentStore == null)
			{
				throw new ArgumentNullException("documentStore");
			}

			var users = new List<User>();
			users.AddRange(new[]
			{
				new User {Id = null, DisplayName = "Fred Smith"},
				new User {Id = null, DisplayName = "Jane Doe"},
				new User {Id = null, DisplayName = "John Doe"},
				new User {Id = null, DisplayName = "Pure Krome"},
				new User {Id = null, DisplayName = "Ayende Rahien"},
				new User {Id = null, DisplayName = "Itamar Syn-Hershko"},
				new User {Id = null, DisplayName = "Oren Eini"},
				new User {Id = null, DisplayName = null} // <--- Assume this is an option field....
			});
			using (IDocumentSession documentSession = documentStore.OpenSession())
			{
				foreach (User user in users)
				{
					documentSession.Store(user);
				}

				documentSession.SaveChanges();
			}
		}
	}
}