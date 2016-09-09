using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Imports.Newtonsoft.Json;
using Xunit;

namespace SlowTests.MailingList
{
    public class Pfeffer : RavenTestBase
    {
        [Fact]
        public void QueryingUsingObjects()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.CustomizeJsonSerializer += serializer =>
                {
                    serializer.TypeNameHandling = TypeNameHandling.All;
                };
                using (var session = store.OpenSession())

                {
                    var obj = new Outer { Examples = new List<IExample>() { new Example { Provider = "Test", Id = "Abc" } } };
                    session.Store(obj);
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var ex = new Example { Provider = "Test", Id = "Abc" };
                    var arr = session.Query<Outer>().Customize(c => c.WaitForNonStaleResults())
                        .Where(o => o.Examples.Any(e => e == ex))
                        .ToArray();
                    //WaitForUserToContinueTheTest(store);
                    Assert.Equal(1, arr.Length);
                }
            }
        }

        // Define other methods and classes here
        private interface IExample
        {
        }

        private class Outer
        {
            public int Id { get; set; }
            public IList<IExample> Examples { get; set; }
        }

        private class Example : IExample
        {
            public string Provider { get; set; }
            public string Id { get; set; }
        }
    }
}
