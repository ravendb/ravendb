using FastTests;
using Newtonsoft.Json.Linq;
using Sparrow.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15764 : RavenTestBase
    {
        public RavenDB_15764(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CsharpRecordSerialization_ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person("John", "Doe"), "people/1");

                    session.SaveChanges();

                    Assert.False(session.Advanced.HasChanges);

                    var numberOfRequests = session.Advanced.NumberOfRequests;
                    session.SaveChanges();

                    Assert.Equal(session.Advanced.NumberOfRequests, numberOfRequests);
                }

                using (var session = store.OpenSession())
                {
                    var person = session.Load<Person>("people/1");

                    Assert.NotNull(person);
                    Assert.Equal("John", person.FirstName);
                    Assert.Equal("Doe", person.LastName);

                    Assert.False(session.Advanced.HasChanges);

                    var numberOfRequests = session.Advanced.NumberOfRequests;
                    session.SaveChanges();

                    Assert.Equal(session.Advanced.NumberOfRequests, numberOfRequests);
                }

                using (var session = store.OpenSession())
                {
                    var person = session.Load<JObject>("people/1");

                    var equalityProperty = person[TypeExtensions.RecordEqualityContractPropertyName];
                    Assert.Null(equalityProperty);
                }
            }
        }

        private record Person
        {
            public string LastName { get; }
            public string FirstName { get; }

            public Person(string firstName, string lastName) => (FirstName, LastName) = (firstName, lastName);
        }
    }
}
