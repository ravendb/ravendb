using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_7406 : RavenTestBase
    {
        public RavenDB_7406(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_Create_Index()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new Index());
            }
        }

        public class Index : AbstractIndexCreationTask<Entity>
        {
            public Index()
            {

                Map = entities => from entity in entities
                    let others = entity.Others
                    from other in others
                    select new
                    {
                        Id = entity.Id,
                        Numbers = from number in other.Numbers
                        let str = number.ToString()
                        select number.ToString()
                    };

            }
        }

        public class Entity
        {
            public string Id { get; set; }
            public List<Other> Others { get; set; }
        }

        public class Other
        {
            public List<int> Numbers { get; set; }
        }
    }
}
