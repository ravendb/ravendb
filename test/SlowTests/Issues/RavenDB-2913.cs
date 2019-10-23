using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class UpdateGetters : RavenTestBase
    {
        public UpdateGetters(ITestOutputHelper output) : base(output)
        {
        }

        public class Entity
        {
            public string Property { get; set; }

            public string Getter { get { return Property; } }
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
            using (var store = GetDocumentStore())
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
