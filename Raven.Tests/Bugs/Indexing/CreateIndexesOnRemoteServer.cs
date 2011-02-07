using System.ComponentModel.Composition.Hosting;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class CreateIndexesOnRemoteServer : RemoteClientTest
	{
		[Fact]
		public void CanCreateIndex()
		{
			using(GetNewServer())
			{
				var documentStore = new DocumentStore
				{
					Url = "http://localhost:8080"
				}.Initialize();
				IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(SimpleIndex))), documentStore);
				IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(SimpleIndex))), documentStore);
			}
		}

		public class SimpleIndex : AbstractIndexCreationTask<User>
		{
			public SimpleIndex()
			{
				Map = users => from user in users
				               select new {user.Age};
			}
		}
	}
}