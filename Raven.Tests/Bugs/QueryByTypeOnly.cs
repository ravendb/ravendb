using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class QueryByTypeOnly : RavenTest
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
}