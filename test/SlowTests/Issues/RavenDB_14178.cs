using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14178 : RavenTestBase
    {
        private class Dog
        {
            public string Name { get; set; }

            public List<string> Awards { get; set; }
        }

        public RavenDB_14178(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUseNegatedAnyInLinq()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Dog
                    {
                        Name = "Dog1",
                        Awards = new List<string>()
                    });

                    session.Store(new Dog
                    {
                        Name = "Dog2",
                        Awards = new List<string> { "Medal" }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { NoCaching = true, NoTracking = true }))
                {
                    var dogsQuery = from dog in session.Query<Dog>()
                                    where dog.Awards.Any() == false
                                    select dog;

                    var dogs = dogsQuery.ToList();

                    Assert.Equal(1, dogs.Count);
                    Assert.Equal("Dog1", dogs[0].Name);
                }

                using (var session = store.OpenSession(new SessionOptions { NoCaching = true, NoTracking = true }))
                {
                    var dogsQuery = from dog in session.Query<Dog>()
                                    where !dog.Awards.Any()
                                    select dog;

                    var dogs = dogsQuery.ToList();

                    Assert.Equal(1, dogs.Count);
                    Assert.Equal("Dog1", dogs[0].Name);
                }

                using (var session = store.OpenSession(new SessionOptions { NoCaching = true, NoTracking = true }))
                {
                    var dogsQuery = from dog in session.Query<Dog>()
                                    where dog.Awards.Any() == true
                                    select dog;

                    var dogs = dogsQuery.ToList();

                    Assert.Equal(1, dogs.Count);
                    Assert.Equal("Dog2", dogs[0].Name);
                }

                using (var session = store.OpenSession(new SessionOptions { NoCaching = true, NoTracking = true }))
                {
                    var dogsQuery = from dog in session.Query<Dog>()
                                    where dog.Awards.Any()
                                    select dog;

                    var dogs = dogsQuery.ToList();

                    Assert.Equal(1, dogs.Count);
                    Assert.Equal("Dog2", dogs[0].Name);
                }
            }
        }
    }
}
