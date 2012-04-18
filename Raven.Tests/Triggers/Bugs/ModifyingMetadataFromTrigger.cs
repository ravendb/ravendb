using System;
using System.ComponentModel.Composition.Hosting;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
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
				session.Advanced.DatabaseCommands.OperationsHeaders.Add("CurrentUserPersonId", "1085");

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

				Assert.Equal(AuditTrigger.CreatedAtDateTime, session.Advanced.GetMetadataFor(person).Value<DateTime>("CreatedDate"));
			}

			using (var session = store.OpenSession())
			{
				session.Advanced.DatabaseCommands.OperationsHeaders.Add("CurrentUserPersonId", "1081");

				var person = session.Load<Person>("person/1");
				person.Age = 25;
				session.SaveChanges();

				Assert.Equal(AuditTrigger.CreatedAtDateTime, session.Advanced.GetMetadataFor(person).Value<DateTime>("CreatedDate"));
			}

			using (var session = store.OpenSession())
			{
				session.Advanced.DatabaseCommands.OperationsHeaders.Add("CurrentUserPersonId", "1022");

				var person = session.Load<Person>("person/1");

				person.FirstName = "Steve";
				person.LastName = "Richmond";
				session.SaveChanges();

				Assert.Equal(AuditTrigger.CreatedAtDateTime, session.Advanced.GetMetadataFor(person).Value<DateTime>("CreatedDate"));
			}
		}
	}
}