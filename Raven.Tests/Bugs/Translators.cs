//-----------------------------------------------------------------------
// <copyright file="Translators.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition.Hosting;
using Raven.Client;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Json.Linq;
using Raven.Client.Indexes;
using Xunit;
using System.Linq;
using Raven.Client.Linq;

namespace Raven.Tests.Bugs
{
	public class Translators : RavenTest
	{
		public class Users : AbstractIndexCreationTask<User>
		{
			public Users()
			{
				Map =
					users => from user in users
							 select new {user.Name};

				TransformResults =
					(database, users) => from user in users
										 let partner = database.Load<User>(user.PartnerId)
										 select new {User = user.Name, Partner = partner.Name};
			}
		}

		public class Users_CausingErrors : AbstractIndexCreationTask<User>
		{
			public Users_CausingErrors()
			{
				Map =
					users => from user in users
							 select new { user.Name };

				TransformResults =
					(database, users) => from user in users
										 let partner = database.Load<User>(user.PartnerId)
										 let x = string.Empty[1] // cause errors
										 select new { User = user.Name, Partner = partner.Name };
			}
		}


		[Fact]
		public void CanUseTranslatorToModifyQueryResults_UsingClientGeneratedIndex()
		{
			using (var ds = NewDocumentStore())
			{
				using (var s = ds.OpenSession())
				{
					var entity = new User { Name = "Ayende", Age = 1 };
					s.Store(entity);
					s.Store(new User { Name = "Oren", PartnerId = entity.Id, Age = 1 });
					s.SaveChanges();
				}

				IndexCreation.CreateIndexes(
					new CompositionContainer(new TypeCatalog(typeof (Users))),
					ds);

				using (var s = ds.OpenSession())
				{
					var first = s.Query<User,Users>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x=>x.Name == "Oren")
						.As<UserWithPartner>()
						.First();

					Assert.Equal("Oren", first.User);
					Assert.Equal("Ayende", first.Partner);
				}
			}
		}

		[Fact]
		public void WhenTransformResultsHasAnError()
		{
			using (var ds = NewDocumentStore())
			{
				using (var s = ds.OpenSession())
				{
					var entity = new User { Name = "Ayende" };
					s.Store(entity);
					s.Store(new User { Name = "Oren", PartnerId = entity.Id });
					s.SaveChanges();
				}

				IndexCreation.CreateIndexes(
					new CompositionContainer(new TypeCatalog(typeof(Users_CausingErrors))),
					ds);

				using (var s = ds.OpenSession())
				{
					var exception = Assert.Throws<InvalidOperationException>(() => s.Query<User, Users_CausingErrors>()
																								 .Customize(x => x.WaitForNonStaleResults())
																								 .Where(x => x.Name == "Ayende")
																								 .As<UserWithPartner>()
																								 .First());

					var expectedError = Assert.Throws<IndexOutOfRangeException>(() => string.Empty[1]);

					Assert.Equal("The transform results function failed.\r\nDoc 'users/1', Error: " + expectedError.Message, exception.Message);
				}
			}
		}

		private class UserWithPartner
		{
			public string User { get; set; }
			public string Partner { get; set; }
		}

		[Fact]
		public void CanUseTranslatorToModifyQueryResults()
		{
			using(var ds = NewDocumentStore())
			{
				using(var s = ds.OpenSession())
				{
					s.Store(new User {Name = "Ayende"});
					s.SaveChanges();
				}

				ds.DatabaseCommands.PutIndex("Users",
											 new IndexDefinition
											 {
												 Map = "from u in docs.Users select new { u.Name }",
												 TransformResults = "from user in results select new { Name = user.Name.ToUpper() }"
											 });


				using (var s = ds.OpenSession())
				{
					var first = s.Query<RavenJObject>("Users").Customize(x=>x.WaitForNonStaleResults())
						.First();

					Assert.Equal("AYENDE", first.Value<string>("Name"));
				}
			}
		}

		[Fact]
		public void CanUseTranslatorToLoadAnotherDocument()
		{
			using (var ds = NewDocumentStore())
			{
				using (var s = ds.OpenSession())
				{
					var entity = new User { Name = "Ayende" };
					s.Store(entity);
					s.Store(new User { Name = "Oren", PartnerId = entity.Id});
					s.SaveChanges();
				}

				ds.DatabaseCommands.PutIndex("Users",
											 new IndexDefinition
											 {
												 Map = "from u in docs.Users select new { u.Name }",
												 TransformResults =
												 @"
from user in results 
let partner = Database.Load(user.PartnerId)
select new { Name = user.Name, Partner = partner.Name }"
											 });


				using (var s = ds.OpenSession())
				{
					var first = s.Advanced.LuceneQuery<RavenJObject>("Users")
						.WaitForNonStaleResults()
						.WhereEquals("Name", "Oren", true)
						.First();

					Assert.Equal(@"{""Name"":""Oren"",""Partner"":""Ayende""}", first.ToString(Formatting.None));
				}
			}
		}
	}
}
