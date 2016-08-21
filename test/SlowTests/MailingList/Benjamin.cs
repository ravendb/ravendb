using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class Benjamin : RavenTestBase
    {
        private class Person
        {
            public PersonFace Face;
        }

        private class PersonFace
        {
            public string Color { get; set; }
        }

        [Fact]
        public async Task Can_project_nested_objects()
        {
            using (var store = await GetDocumentStore())
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
