using System;
using System.ComponentModel.Composition.Hosting;

using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Triggers.Bugs
{
	public class ModifyingMetadataFromTrigger : RavenTest
	{
		private readonly IDocumentStore store;

		public ModifyingMetadataFromTrigger()
		{
			store = NewRemoteDocumentStore(databaseName: Constants.SystemDatabase);
		}

		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.Admin;
			configuration.Catalog.Catalogs.Add(new TypeCatalog(typeof (AuditTrigger)));
		}

		[Fact]
		public void WillNotCorruptData()
		{
			using (var session = store.OpenSession())
			{
				store.DatabaseCommands.OperationsHeaders.Add("CurrentUserPersonId", "1085");

				var person = new Person
				{
					Id = "person/1",
					FirstName = "Nabil",
					LastName = "Shuhaiber",
					Age = 31,
					Title = "Vice President"
				};

				session.Store(person);
				session.SaveChanges();

				TestCreatedDate(session.Advanced.GetMetadataFor(person));
			}

			using (var session = store.OpenSession())
			{
				store.DatabaseCommands.OperationsHeaders.Add("CurrentUserPersonId", "1081");

				var person = session.Load<Person>("person/1");
				person.Age = 25;
				session.SaveChanges();

				TestCreatedDate(session.Advanced.GetMetadataFor(person));
			}

			using (var session = store.OpenSession())
			{
				store.DatabaseCommands.OperationsHeaders.Add("CurrentUserPersonId", "1022");

				var person = session.Load<Person>("person/1");

				person.FirstName = "Steve";
				person.LastName = "Richmond";
				session.SaveChanges();

				TestCreatedDate(session.Advanced.GetMetadataFor(person));
			}
		}

		[Fact]
		public void WillLoadTheSameDateThatWeStored()
		{
			using (var session = store.OpenSession())
			{
				var person = new Person
				{
					Id = "person/1",
					FirstName = "Nabil",
					LastName = "Shuhaiber",
					Age = 31,
					Title = "Vice President"
				};

				session.Store(person);
				session.SaveChanges();

				TestCreatedDate(session.Advanced.GetMetadataFor(person));
			}

			using (var session = store.OpenSession())
			{
				var person = session.Load<Person>("person/1");
				TestCreatedDate(session.Advanced.GetMetadataFor(person));
			}
		}

		private void TestCreatedDate(RavenJObject metadata)
		{
			var createdDate = metadata.Value<DateTime>("CreatedDate");
			Assert.Equal(AuditTrigger.CreatedAtDateTime, createdDate);
		}
	}
}