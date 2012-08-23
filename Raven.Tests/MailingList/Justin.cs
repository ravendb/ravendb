using System;
using Raven.Client.Linq;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Justin : RavenTest
	{
		[Fact]
		public void ActualTest()
		{
			// Arrange.
			using (var documentStore = NewDocumentStore())
			{

				new Users_NameAndPassportSearching().Execute(documentStore);

				var users = CreateFakeUsers();
				var usersCount = users.Count();
				using (var documentSession = documentStore.OpenSession())
				{
					foreach (var user in users)
					{
						documentSession.Store(user);
					}
					documentSession.SaveChanges();
				}


				// If we want to search for *Krome .. this means the index will contain
				// 'emorK eruP' .. so we need to reverse the search query string.
				var userSearchQuery = new string("Krome".Reverse().ToArray());
				var passportSearchQuery = new string("12345".Reverse().ToArray());


				// Act.

				// Lets check if there are any errors.
				var errors = documentStore.DatabaseCommands.GetStatistics().Errors;
				Assert.Empty(errors);

				using (var documentSession = documentStore.OpenSession())
				{
					var allData = documentSession
						.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.ToList();

					Assert.Equal(usersCount, allData.Count);

					var specificUsers = documentSession
						.Query<Users_NameAndPassportSearching.ReduceResult, Users_NameAndPassportSearching>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.ReversedName.StartsWith(userSearchQuery))
						.As<User>()
						.ToList();

					var passports = documentSession
						.Query<Users_NameAndPassportSearching.ReduceResult, Users_NameAndPassportSearching>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.ReversedName.StartsWith(passportSearchQuery))
						.ToList();

				}
			}
		}

		[Fact]
		public void ActualTest_IgnoreErrors()
		{
			// Arrange.
			using (var server = GetNewServer())
			using (var documentStore = new DocumentStore { Url = server.Database.Configuration.ServerUrl }.Initialize())
			{
				Console.WriteLine("Document Store initialized - running in memory.");

				Assert.Throws<InvalidOperationException>(() => new Users_NameAndPassportSearching_WithError().Execute(documentStore));

				var users = CreateFakeUsers();
				var usersCount = users.Count();
				using (var documentSession = documentStore.OpenSession())
				{
					foreach (var user in users)
					{
						documentSession.Store(user);
					}
					documentSession.SaveChanges();
				}
				Console.WriteLine("Seed data stored.");

				// Act.

				// Lets check if there are any errors.
				var errors = documentStore.DatabaseCommands.GetStatistics().Errors;
				if (errors != null && errors.Length > 0)
				{
					foreach (var error in errors)
					{
						Console.WriteLine("Index: {0}; Error: {1}", error.Index, error.Error);
					}

					return;
				}
				Console.WriteLine("No Document Store errors.");

				using (var documentSession = documentStore.OpenSession())
				{
					var allData = documentSession
						.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.ToList();

					Assert.Equal(usersCount, allData.Count);

					foreach (var user in allData)
					{
						Console.WriteLine("User: {0}; Passport: {1}", user.Name, user.PassportNumber);
					}
				}

				// Assert.
			}
		}

		private static IEnumerable<User> CreateFakeUsers()
		{
			return new List<User>
					   {
						   new User
							   {
								   Name = "Pure Krome",
								   Age = 36,
								   PassportNumber = "QWERTY-12345"
							   },
						   new User
							   {
								   Name = "Ayende Rayen",
								   Age = 35,
								   PassportNumber = "ABC-12345"
							   },
						   new User
							   {
								   Name = "Itamar Syn-Hershko",
								   Age = 34,
								   PassportNumber = "DEF-12345"
							   },
						   new User
							   {
								   Name = "aaa bbb",
								   Age = 33,
								   PassportNumber = "GHI-12345"
							   },
						   new User
							   {
								   Name = "ccc ddd",
								   Age = 32,
								   PassportNumber = "JKL-12345"
							   },
						   new User
							   {
								   Name = "eee fff",
								   Age = 31,
								   PassportNumber = "MNO-12345"
							   }
					   };
		}

		public class User
		{
			public string Name { get; set; }
			public int Age { get; set; }
			public string PassportNumber { get; set; }
		}

		internal class Users_NameAndPassportSearching_WithError : AbstractIndexCreationTask<User, Users_NameAndPassportSearching.ReduceResult>
		{
			public Users_NameAndPassportSearching_WithError()
			{
				Map = users => from user in users
							   select new
							   {
								   user.Name,
								   user.PassportNumber,
								   ReversedName = user.Name,
								   ReversedPassportNumber = user.PassportNumber,
							   };

				// This result function will cause RavenDB to throw an error
				Reduce = results => from r in results
									select new
									{
										r.Name,
										r.PassportNumber,
										ReversedName = r.Name,
									};
			}
		}

		internal class Users_NameAndPassportSearching : AbstractIndexCreationTask<User, Users_NameAndPassportSearching.ReduceResult>
		{
			public Users_NameAndPassportSearching()
			{
				Map = users => from user in users
							   select new
							   {
								   user.Name,
								   user.PassportNumber,
								   ReversedName = user.Name.Reverse(),
								   ReversedPassportNumber = user.PassportNumber.Reverse(),
							   };
			}

			#region Nested type: ReduceResult

			public class ReduceResult
			{
				public string Name { get; set; }
				public string PassportNumber { get; set; }
				public string ReversedName { get; set; }
				public string ReversedPassportNumber { get; set; }
			}

			#endregion
		}
	}
}
