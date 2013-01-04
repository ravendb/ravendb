using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs.Identifiers
{
	public class LongId : RavenTest
	{
		public class Entity
		{
			public long Id { get; set; }
		}

		[Fact]
		public void Can_load_entity()
		{
			using(var store = NewDocumentStore())
			{
				object id;
				using (var session = store.OpenSession())
				{
					var entity = new Entity();
					session.Store(entity);
					id = entity.Id;
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var entity1 = session.Load<Entity>("entities/"+id);
					Assert.NotNull(entity1);
				}
			}
		}
	}
}
