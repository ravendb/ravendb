using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Benjamin : RavenTestBase
    {
        public Benjamin(ITestOutputHelper output) : base(output)
        {
        }

        private class Person
        {
            public PersonFace Face;
        }

        private class PersonFace
        {
            public string Color { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Can_project_nested_objects(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Face = new PersonFace
                        {
                            Color = "Green"
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var faces = session.Query<Person>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Select(x => x.Face)
                        .ToList();

                    Assert.Equal("Green", faces[0].Color);
                }
            }
        }
    }
}
