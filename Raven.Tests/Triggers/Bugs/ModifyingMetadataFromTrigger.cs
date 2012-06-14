using System;
using System.ComponentModel.Composition.Hosting;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Json.Linq;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Triggers.Bugs
{
	public class ModifyingMetadataFromTrigger : RemoteClientTest
	{
		private readonly string path;
		private readonly RavenDbServer ravenDbServer;
		private readonly IDocumentStore store;

		public ModifyingMetadataFromTrigger()
		{
			path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8079);

			ravenDbServer = new RavenDbServer(new RavenConfiguration
			                                  	{
			                                  		Port = 8079,
			                                  		DataDirectory = path,
			                                  		AnonymousUserAccessMode = AnonymousUserAccessMode.All,
			                                  		Catalog =
			                                  			{
			                                  				Catalogs = {new TypeCatalog(typeof (AuditTrigger))}
			                                  			}
			                                  	});
			store = new DocumentStore {Url = "http://localhost:8079"}.Initialize();
		}

		public override void Dispose()
		{
			store.Dispose();
			ravenDbServer.Dispose();
			IOExtensions.DeleteDirectory(path);
			base.Dispose();
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
			Assert.Equal(DateTimeKind.Unspecified, createdDate.Kind);
			Assert.Equal(AuditTrigger.CreatedAtDateTime, createdDate);
		}
	}
}