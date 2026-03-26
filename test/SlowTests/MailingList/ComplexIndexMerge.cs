using System;
using System.Linq;
using FastTests;
using Xunit;
using Tests.Infrastructure;

namespace SlowTests.MailingList
{
    public class ComplexIndexMerge : RavenTestBase
    {
        public ComplexIndexMerge(ITestOutputHelper output) : base(output)
        {
        }

        private class Ref
        {
            public Guid Id { get; set; }
        }

        private class Entity
        {
            public Ref EntityARef { get; set; }
            public Ref EntityBRef { get; set; }
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanQueryOnBothProperties()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Entity
                    {
                        EntityARef = new Ref(),
                        EntityBRef = new Ref()
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var id = Guid.Empty;
                    Assert.NotEmpty(session.Query<Entity>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.EntityARef.Id == id).ToList());
                    Assert.NotEmpty(session.Query<Entity>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.EntityBRef.Id == id).ToList());
                    Assert.NotEmpty(session.Query<Entity>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.EntityARef.Id == id).ToList());
                }
            }
        }
    }
}