using Xunit;

namespace NewClientTests.NewClient.Raven.Tests.Bugs.Identifiers
{
    public class LongId : RavenTestBase
    {
        public class Entity
        {
            public long Id { get; set; }
        }

        [Fact(Skip = "NotImplementedException")]
        public void Can_load_entity()
        {
            using (var store = GetDocumentStore())
            {
                object id;
                using (var session = store.OpenNewSession())
                {
                    var entity = new Entity();
                    session.Store(entity);
                    id = entity.Id;
                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    var entity1 = session.Load<Entity>("entities/" + id);
                    Assert.NotNull(entity1);
                }
            }
        }
    }
}
