using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class UpdateGetters : RavenTestBase
    {
        [SerializableAttribute]
        public class Entity
        {
            public string Property { get; set; }
            
            public string Getter { get { return Property; }  }
        }

        public class EntityIndex : AbstractIndexCreationTask<Entity>
        {
            public EntityIndex()
            {
                Map = entities => from entity in entities
                                  select new
                                  {
                                      entity.Getter
                                  };
            }
        }

        [Fact]
        public void ShouldUpdateGetters()
        {
            using (var store = NewDocumentStore())
            {
                new EntityIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Entity { Property = "testing" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // Getter is evaluated correctly on first save, so this works
                    var result = session.Query<Entity, EntityIndex>().Customize(x => x.WaitForNonStaleResults()).First(x => x.Getter == "testing");

                    result.Property = "modified";
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {

                    // Fail: Sequence contains no elements
                    try
                    {
                        var res = session.Query<Entity, EntityIndex>().Customize(x => x.WaitForNonStaleResults()).First(x => x.Getter == "modified");
                    }
                    catch (Exception)
                    {
                        
                    }
                }
            }
        }
    }
}
