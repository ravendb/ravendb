using System;
using System.Linq;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12516: RavenTestBase
    {
        public RavenDB_12516(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name { get; set; } 
            public int[] CoworkerIds { get; set; }
        }

        [Fact]
        public void Should_throw_if_using_javascript_select_in_edge_expression()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(@"
                        match (Users as u1 where id() = 'users/2')-
                            [CoworkerIds as ids
                                select { coworkerIds: ids.map(x => 'users/' + x) }]
                             ->(Users as u2)
                    ").ToList());
                }
            }
        }

        private static void CreateData(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User
                {
                    Name = "John Doe",
                    CoworkerIds = new[] {1}
                });

                session.Store(new User
                {
                    Name = "Jane Doe",
                    CoworkerIds = Array.Empty<int>()
                });
                session.SaveChanges();
            }
        }
    }
}
