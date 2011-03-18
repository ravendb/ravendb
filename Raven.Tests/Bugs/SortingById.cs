using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class SortingById : RemoteClientTest
	{
		[Fact]
		public void ShouldBePossible()
		{
			using (GetNewServer())
			using (IDocumentStore store = new DocumentStore {Url = "http://localhost:8080"}.Initialize())
			{
				using (IDocumentSession session = store.OpenSession())
				{
					session.Query<SerializingEntities.Product>().OrderBy(w => w.Id).Skip(5).Take(5).ToArray();
				}
			}
		}
	}
}