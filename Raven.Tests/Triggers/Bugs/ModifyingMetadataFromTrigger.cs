using System;
using System.ComponentModel.Composition.Hosting;
using System.Diagnostics;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Http;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Triggers.Bugs
{
	public class ModifyingMetadataFromTrigger : RemoteClientTest, IDisposable
	{
		private readonly string path;
		private readonly int port;

		public ModifyingMetadataFromTrigger()
		{
			port = 8080;
			path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8080);
		}

		#region IDisposable Members

		public void Dispose()
		{
			IOExtensions.DeleteDirectory(path);
		}

		#endregion

		[Fact]
		public void WillNotCorruptData()
		{
			using ( new RavenDbServer(new RavenConfiguration
			{
				Port = port, 
				DataDirectory = path, 
				AnonymousUserAccessMode = AnonymousUserAccessMode.All,
				Catalog =
					{
						Catalogs = {new TypeCatalog(typeof(AuditTrigger))}
					}
			}))
			using (var store = new DocumentStore {Url = "http://localhost:8080"})
			{
				store.Initialize();

				using (IDocumentSession session = store.OpenSession())
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

					Assert.Equal(new DateTime(2011, 02, 19, 15, 00, 00), session.Advanced.GetMetadataFor(person).Value<DateTime>("CreatedDate"));
				}

				using (IDocumentSession session = store.OpenSession())
				{
					session.Advanced.DatabaseCommands.OperationsHeaders.Add("CurrentUserPersonId", "1081");

					var person = session.Load<Person>("person/1");
					person.Age = 25;
					session.SaveChanges();

					Assert.Equal(new DateTime(2011, 02, 19, 15, 00, 00), session.Advanced.GetMetadataFor(person).Value<DateTime>("CreatedDate"));
				}

				using (IDocumentSession session = store.OpenSession())
				{
					session.Advanced.DatabaseCommands.OperationsHeaders.Add("CurrentUserPersonId", "1022");

					var person = session.Load<Person>("person/1");

					person.FirstName = "Steve";
					person.LastName = "Richmond";
					session.SaveChanges();

					Assert.Equal(new DateTime(2011, 02, 19, 15, 00, 00), session.Advanced.GetMetadataFor(person).Value<DateTime>("CreatedDate"));
				}
			}
		}
	}
}