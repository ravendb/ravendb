using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Bugs.Async
{
	public class DynamicGeneratedIds : RemoteClientTest
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
