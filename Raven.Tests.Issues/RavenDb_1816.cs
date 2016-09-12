using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDb_1816 : RavenTest
    {
        [Fact]
        public void CanLoadLongQuerry()
        {
            List<int> list = Enumerable.Range(1, 3000).ToList();
            using (var server = GetNewServer(configureConfig: configuration =>
            {
                configuration.MaxClauseCount = 6000;
            }))
            using (IDocumentStore store = NewRemoteDocumentStore(ravenDbServer: server))
            {
                new LaptopIndex().Execute(store);
                using (IDocumentSession session = store.OpenSession())
                {
                    IRavenQueryable<Laptop> q = session.Query<Laptop, LaptopIndex>()
                        .Where(x => x.Id.In(list));

                    var s = q.ToString();

                    Assert.True(s.Length > (32*1024));

                    using (IEnumerator<StreamResult<Laptop>> streamingQuery = session.Advanced.Stream(q))
                    {
                        Assert.False(streamingQuery.MoveNext());
                    }
                }
            }
        }

        public class LaptopIndex : AbstractIndexCreationTask<Laptop>
        {
            public LaptopIndex()
            {
                Map = laptops => from laptop in laptops
                                 select new
                                 {
                                     laptop.Id
                                 };
            }
        }

        public class Laptop
        {
            public int Id { get; set; }
            public string Cpu { get; set; }
            public string Manufacturer { get; set; }
            public string HDDSizeInGigabytes { get; set; }
            public string RamSizeInMegabatye { get; set; }
        }
    }
}
