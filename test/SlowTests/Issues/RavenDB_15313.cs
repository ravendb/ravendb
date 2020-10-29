using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.Counters;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15313 : RavenTestBase
    {
        public RavenDB_15313(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void GetCountersOperationShouldFilterDuplicateNames()
        {
            using (var store = GetDocumentStore())
            {
                var docId = "users/1";

                var names = new []
                {
                    "likes", "dislikes", "likes", "downloads", "likes", "downloads"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new object(), docId);

                    var cf = session.CountersFor(docId);

                    for (int i = 0; i < names.Length; i++)
                    {
                        cf.Increment(names[i], i);
                    }

                    session.SaveChanges();
                }

                var vals = store.Operations.Send(new GetCountersOperation(docId, names));

                Assert.Equal(3, vals.Counters.Count);

                var expected = 6; // likes
                Assert.Equal(expected, vals.Counters[0].TotalValue);

                expected = 1; // dislikes
                Assert.Equal(expected, vals.Counters[1].TotalValue);

                expected = 8; // downloads
                Assert.Equal(expected, vals.Counters[2].TotalValue);
            }
        }

        [Fact]
        public void GetCountersOperationShouldFilterDuplicateNames_PostGet()
        {
            using (var store = GetDocumentStore())
            {
                var docId = "users/1";

                var names = new string[1024];
                var dict = new Dictionary<string, int>();

                using (var session = store.OpenSession())
                {
                    session.Store(new object(), docId);

                    var cf = session.CountersFor(docId);

                    for (int i = 0; i < 1024; i++)
                    {
                        string name;
                        if (i % 4 == 0)
                        {
                            name = "abc";
                        }
                        else if (i % 10 == 0)
                        {
                            name = "xyz";
                        }
                        else
                        {
                            name = "likes" + i;
                        }

                        names[i] = name;

                        dict.TryGetValue(name, out var oldVal);
                        dict[name] = oldVal + i;

                        cf.Increment(name, i);
                    }


                    session.SaveChanges();
                }

                var vals = store.Operations.Send(new GetCountersOperation(docId, names));

                var expectedCount = dict.Count;
                Assert.Equal(expectedCount, vals.Counters.Count);

                var hs = new HashSet<string>(names);
                var expectedVals = names
                    .Where(name => hs.Remove(name))
                    .Select(name => dict[name])
                    .ToList();

                for (var i = 0; i < vals.Counters.Count; i++)
                {
                    Assert.Equal(expectedVals[i], vals.Counters[i].TotalValue);
                }
            }
        }
    }
}
