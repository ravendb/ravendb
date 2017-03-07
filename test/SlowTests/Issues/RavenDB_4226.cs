using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Transformers;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4226 : RavenTestBase
    {
        private class A
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class B
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class A2B : AbstractTransformerCreationTask<A>
        {
            public A2B()
            {
                TransformResults = entities => from entity in entities
                                               let idAsStr = entity.Id.ToString()
                                               select new { Id = idAsStr.Remove(0, 3), Name = entity.Name.Reverse() };
            }
        }

        [Fact]
        public void QueryShouldRecpectOriginalQueryTypeNotTransformerType()
        {
            var ids = new List<string> { "as/1", "as/2", "as/3" };
            using (var store = GetDocumentStore())
            {
                store.ExecuteTransformer(new A2B());
                using (var session = store.OpenSession())
                {
                    session.Store(new A { Id = "as/7", Name = "Shmulick" });
                    session.Store(new A { Id = "as/2", Name = "Itzik" });
                    session.Store(new A { Id = "as/11", Name = "Shalom" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var res = session.Query<A>().Where(x => x.Id.In(ids)).TransformWith<A2B, B>()
                        .Customize(x => x.WaitForNonStaleResults()).Customize(x => x.NoCaching()).Single();
                    Assert.Equal("kiztI", res.Name);
                }
            }
        }
    }
}