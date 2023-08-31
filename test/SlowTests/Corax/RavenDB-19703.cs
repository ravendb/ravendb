using FastTests;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax
{
    public class RavenDB_19703 : RavenTestBase
    {
        public RavenDB_19703(ITestOutputHelper output) : base(output)
        {
        }

        private record Person(string Name);

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanStoreNullValueInsideAutoIndex(Options options)
        {
            using var store = GetDocumentStore(options);
            var person = new Person(Encodings.Utf8.GetString(new byte[] { (byte)'m', (byte)'a', (byte)'c', (byte)'\0', (byte)'e', (byte)'\0' }));
            using (var session = store.OpenSession())
            {
                session.Store(person);
                session.SaveChanges();

                var result = session.Advanced
                    .DocumentQuery<Person>()
                    .WaitForNonStaleResults()
                    .WhereEndsWith(i => i.Name, Encodings.Utf8.GetString(new byte[] { (byte)'\0' }))
                    .ToList();
                WaitForUserToContinueTheTest(store);
                Assert.Equal(1, result.Count);
                Assert.Equal(person.Name, result[0].Name);
            }
        }
    }
}

