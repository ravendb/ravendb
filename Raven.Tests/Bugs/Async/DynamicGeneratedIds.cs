using System.Dynamic;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Bugs.Async
{
	public class DynamicGeneratedIds : RavenTest
	{
		[Fact]
		public void AsyncMatchesSyncGeneratedIdBehavior()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					dynamic client = new ExpandoObject();
					client.Name = "Test";
					session.Store(client);

					Assert.Equal("ExpandoObjects/1", client.Id);
				}
			}
		}
	}
}
