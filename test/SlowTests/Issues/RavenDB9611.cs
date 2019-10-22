using System.Collections.Generic;
using System.Linq;
using FastTests;
using Newtonsoft.Json.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB9611 : RavenTestBase
    {
        public RavenDB9611(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name;
            public string[] Groups;
        }

        private class Group
        {
            public string[] Tags;
        }

        [Fact]
        public void CanJoinArraysDuringQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Group
                    {
                        Tags = new[] { "Admins", "Root", "Baron" }
                    }, "groups/admins");
                    s.Store(new Group
                    {
                        Tags = new[] { "Standard", "Normal", "Just Joe" }
                    }, "groups/std-users");

                    s.Store(new User
                    {
                        Name = "Oren",
                        Groups = new[] { "groups/admins", "groups/std-users" }
                    });

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var results = s.Advanced.RawQuery<dynamic>(@"
from Users as u
load u.Groups as g[]
select g.Tags, u.Name
").ToList();
                    Assert.Equal(1, results.Count);
                    var expected = new HashSet<object>
                    {
                        "Standard",
                        "Normal",
                        "Just Joe",
                        "Admins",
                        "Root",
                        "Baron"
                    };
                    var actual = new HashSet<object>(
                        ((IEnumerable<object>)results[0].Tags).Cast<JValue>()
                            .Select(x=>x.Value<string>()));
                    Assert.Equal(expected, actual);

                }
            }
        }
    }
}
