using System.Linq;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class QueryByTypeOnly : LocalClientTest
	{
		[Fact]
		public void QueryOnlyByType()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Query<SerializingEntities.Product>()
					   .Skip(5)
					   .Take(5)
					   .ToList();
				}
			}
		}
	}

	public class QueryByTypeOnlyRemote : RemoteClientTest
	{
		[Fact]
		public void QueryOnlyByType()
		{
			using (GetNewServer())
			using (var store = new DocumentStore{Url = "http://localhost:8080"}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Query<SerializingEntities.Product>()
					   .Skip(5)
					   .Take(5)
					   .ToList();
				}
			}
		}
	}
}