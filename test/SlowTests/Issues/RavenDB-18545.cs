using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18545 : RavenTestBase
    {
        public RavenDB_18545(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task QuotationForGroupInAlias()
        {
            using (var store = GetDocumentStore())
            {
                CancellationToken cancellationToken = CancellationToken.None;
                using (var session = store.OpenAsyncSession())
                {
                    var job = new Job { Name = "HR Worker", Group = "HR" };
                    await session.StoreAsync(job);
                    string jobId = job.Id;
                    await session.SaveChangesAsync();

                    var q = session.Query<Job>()
                        .GroupBy(j => j.Group)
                        .Select(x => new {Group = x.Key});

                    Assert.EndsWith("as \'Group\'", q.ToString());

                    var l = (await q
                                        .ToListAsync(cancellationToken))
                                        .Select(x => x.Group).ToList();

                    Assert.NotEmpty(l);
                    Assert.Equal(1, l.Count);
                    Assert.Equal(l[0], job.Group);
                }
            }
        }

        private class Job
        {
            public string Id { get; set; }
            public string Name { get; set;  }
            public string Group { get; set; }
        }
    }

}
