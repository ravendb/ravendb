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
    public class RavenDB_4231 : RavenTest
    {
        public class A
        {
             public int Id { get; set; }
        }

        public class B
        {
            public int Id { get; set; }
        }

        public class A2B : AbstractTransformerCreationTask<A>
        {
            public A2B()
            {
                TransformResults = entities => from entity in entities select new {Id = entity.Id};
            }

        } 
        [Fact]
        public void QueryShouldRecpectOriginalQueryTypeNotTransformerType()
        {
            var ids = new List<int> {1, 2, 3};
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    Assert.Equal("@in<__document_id>:(as/1,as/2,as/3)",
                        session.Query<A>().Where(x => x.Id.In(ids)).TransformWith<A2B,B>().ToString());
                }
            }
        }
    }
}
