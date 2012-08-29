using Raven.Json.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Entities
{
	public class JObjectEntity : RavenTest
	{
		[Fact]
		public void CanUseJObjectAsEntityDirectly()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					s.Store(RavenJObject.FromObject(new {Name = "Ayende"}));
					s.SaveChanges();
				}

				using(var s = store.OpenSession())
				{
					var jObject = s.Advanced.LuceneQuery<RavenJObject>().WaitForNonStaleResults().First();
					Assert.Equal("Ayende", jObject.Value<string>("Name"));
				}
			}
		}
	}
}