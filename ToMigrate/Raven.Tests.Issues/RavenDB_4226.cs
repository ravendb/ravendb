using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Raven.Tests.MailingList;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4226 : RavenTest
    {
        public class A
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }

        public class B
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }

        public class A2B : AbstractTransformerCreationTask<A>
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
            var ids = new List<int> { 1, 2, 3 };
            using (var store = NewRemoteDocumentStore())
            {
                store.ExecuteTransformer(new A2B());
                using (var session = store.OpenSession())
                {
                    session.Store(new A { Id = 7, Name = "Shmulick" });
                    session.Store(new A { Id = 2, Name = "Itzik" });
                    session.Store(new A { Id = 11, Name = "Shalom" });
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