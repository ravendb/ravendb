using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14328 : RavenTestBase
    {
        [Fact]
        public void LoadingDocumentIncludingReferenceNestedInDictionary()
        {
            using (var store = GetDocumentStore())
            {
                string aId = "";
                string bId = "";

                using (var session = store.OpenSession())
                {
                    var a = new EntityA();
                    var b = new EntityB();
                    session.Store(a);
                    session.Store(b);
                    aId = a.Id;
                    bId = b.Id;

                    a.Things.Add("not_important", new EntityA2 { EntityBId = b.Id });

                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    session.Include<EntityA>(r => r.Things.Select(r => r.Value.EntityBId)).Load(aId);
                    var b = session.Load<EntityB>(bId);
                    Assert.NotNull(b);
                }
            }
        }


        public class EntityA
        {
            public string Id { get; set; }
            public Dictionary<string, EntityA2> Things { get; set; }

            public EntityA()
            {
                Things = new Dictionary<string, EntityA2>();
            }
        }

        public class EntityA2
        {
            public string EntityBId { get; set; }
        }

        public class EntityB
        {
            public string Id { get; set; }
        }

        public RavenDB_14328(ITestOutputHelper output) : base(output)
        {
        }
    }
}
