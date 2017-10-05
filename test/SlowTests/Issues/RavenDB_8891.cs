using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8891 : RavenTestBase
    {
        [Fact]
        public void Can_query_multidimentional_array()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Song
                    {
                        Tags = new List<List<string>>
                        {
                            new List<string>
                            {
                                "Elektro House",
                                "100"
                            }
                        }
                    });

                    session.SaveChanges();

                    var results = session.Advanced.RawQuery<Song>(@"from Songs where Tags[][] == ""Elektro House""").WaitForNonStaleResults().ToList();

                    Assert.Equal(1, results.Count);

                    results = session.Query<Song>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Tags.Any(y => y.Any(z => z == "Elektro House"))).ToList();

                    Assert.Equal(1, results.Count);
                }
            }
        }

        private class Song
        {
            public List<List<string>> Tags { get; set; }
        }
    }
}
