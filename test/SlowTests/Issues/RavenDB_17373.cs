using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17373 : RavenTestBase 
    {
        public RavenDB_17373(ITestOutputHelper output) : base(output)
        {
        }

        public class User
        {
            public string Name;
            public DateTime Registered;
        }

        [Fact]
        public void CanGetSameResultOnRangeQueryMultipleTimes()
        {
            var store = GetDocumentStore();
            using (var s = store.OpenSession())
            {
                var start = new DateTime(2021, 1, 1);
                for (int i = 0; i < 10; i++)
                {
                    s.Store(new User
                    {
                        Name = "users/" + (i%3),
                        Registered = start.AddDays(i)
                    });
                }
                s.SaveChanges();
            }

            using (var s = store.OpenSession())
            {
                var results = s.Query<User>()
                    .Where(x => x.Name == "users/1" && x.Registered >= new DateTime(2021, 1, 1) && x.Registered <= new DateTime(2021, 1, 8))
                    .ToList();
                Assert.Equal(3, results.Count);
             
                // now we'll cache the query
                results = s.Query<User>()
                    .Where(x => x.Name == "users/2" && x.Registered >= new DateTime(2021, 1, 1) && x.Registered <= new DateTime(2021, 1, 8))
                    .ToList();
                Assert.Equal(2, results.Count); // the last users/2 isn't on the range 
                
                results = s.Query<User>()
                    .Where(x => x.Name == "users/0" && x.Registered >= new DateTime(2021, 1, 1) && x.Registered <= new DateTime(2021, 1, 8))
                    .ToList();
                Assert.Equal(3, results.Count);
            }
        }
    }
}
