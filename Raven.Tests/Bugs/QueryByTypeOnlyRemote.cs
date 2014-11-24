using System.Linq;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class QueryByTypeOnlyRemote : RavenTest
	{
		[Fact]
		public void QueryOnlyByType()
		{
			using (GetNewServer())
			using (var store = new DocumentStore{Url = "http://localhost:8079"}.Initialize())
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