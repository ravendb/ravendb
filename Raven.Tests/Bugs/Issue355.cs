using Raven.Client.Document;
using Raven.Client.Extensions;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class Issue355 : RemoteClientTest
	{
		private string _ravenUrl = "http://localhost:8079";
		private string _ravenDatabaseName = "Northwind";

		[Fact]
		public void ShouldNotThrow()
		{
			using(GetNewServer())
			{
				EnsureDatabaseExists();	
			}	
		}

		public DocumentStore GetDocumentStore()
		{
			var doc = new DocumentStore();
			doc.Url = _ravenUrl;
			doc.Conventions = new DocumentConvention()
			{
				FindIdentityProperty = p => p.Name.Equals("RavenDocumentId")
			};
			return doc;
		}

		public void EnsureDatabaseExists()
		{
			using (var store = GetDocumentStore())
			{
				store.Initialize();
				store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists(_ravenDatabaseName);
			}
		}

	}
}