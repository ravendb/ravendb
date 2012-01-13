using System;
using Raven.Client.Linq;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Client.Indexes;

namespace ConsoleApplication1
{
	internal class Program
	{
		private static void Main()
		{
			// Arrange.
			var documentStore = new EmbeddableDocumentStore
			{
				RunInMemory = true
			};
			documentStore.Initialize();
			Console.WriteLine("Document Store initialized - running in memory.");

			IndexCreation.CreateIndexes(typeof(Users_NameAndPassportSearching).Assembly, documentStore);
			Console.WriteLine("Indexes defined.");

			var users = CreateFakeUsers(); 
			using (var documentSession = documentStore.OpenSession())
			{
				foreach (var user in users)
				{
					documentSession.Store(user);
				}
				documentSession.SaveChanges();
			}
			Console.WriteLine("Seed data stored.");


			// If we want to search for *Krome .. this means the index will contain
			// 'emorK eruP' .. so we need to reverse the search query string.
			var userSearchQuery = new string("Krome".Reverse().ToArray());
			var passportSearchQuery = new string("12345".Reverse().ToArray());


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

				foreach (var user in allData)
				{
					Console.WriteLine("User: {0}; Passport: {1}", user.Name, user.PassportNumber);
				}

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
					.As<User>()
					.ToList();

				foreach (var user in specificUsers)
				{
					Console.WriteLine("User: {0}; Passport: {1}", user.Name, user.PassportNumber);
				}

				foreach (var passport in passports)
				{
					Console.WriteLine("User: {0}; Passport: {1}", passport.Name, passport.PassportNumber);
				}
			}

			// Assert.
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
	}

	internal class User
	{
		public string Name { get; set; }
		public int Age { get; set; }
		public string PassportNumber { get; set; }
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
							   ReversedName = new string(user.Name.Reverse().ToArray()),
							   ReversedPassportNumber = new string(user.PassportNumber.Reverse().ToArray()),
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