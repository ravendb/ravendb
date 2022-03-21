using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB982 : RavenTestBase
    {
        public RavenDB982(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void WillNotForceValuesToBeString()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Age = 4 });
                    session.Store(new Person { Age = 4 });
                    session.SaveChanges();
                }

                new PeopleByAge().Execute(store);

                Indexes.WaitForIndexing(store);

                using (var commands = store.Commands())
                {
                    var queryResult = commands.Query(new IndexQuery { Query = "FROM INDEX 'PeopleByAge'"});

                    var result = (BlittableJsonReaderObject)queryResult.Results[0];

                    object age;
                    Assert.True(result.TryGet("Age", out age));
                    Assert.IsType(typeof(long), age);
                    Assert.Equal(4L, age);

                    object count;
                    Assert.True(result.TryGetMember("Count", out count));
                    Assert.IsType(typeof(long), count);
                    Assert.Equal(2L, count);
                }
            }
        }

        private class PeopleByAge : AbstractIndexCreationTask<Person, PeopleByAge.Result>
        {
            public class Result
            {
                public int Age { get; set; }
                public int Count { get; set; }
            }

            public PeopleByAge()
            {
                Map = persons =>
                      from person in persons
                      select new
                      {
                          Count = 1,
                          person.Age
                      };
                Reduce = results =>
                         from result in results
                         group result by result.Age
                         into g
                         select new
                         {
                             Count = g.Sum(x => x.Count),
                             Age = g.Key
                         };
            }
        }

        private class Person
        {
            public int Age { get; set; }
        }
    }
}
