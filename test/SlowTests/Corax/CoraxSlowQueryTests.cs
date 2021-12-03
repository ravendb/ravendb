using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using FastTests;
using FastTests.Server.Documents.Indexing;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax
{
    public class CoraxSlowQueryTests : RavenTestBase
    {
        public CoraxSlowQueryTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [SearchEngineClassData(SearchEngineType.Corax)]
        public void CompoundOrderByWithPagination(string searchEngineType, int size = 10_000)
        {
            List<Result> expected = new();
            var option = new Options()
            {
                ModifyDatabaseRecord = d =>
                {
                    d.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = searchEngineType;
                    d.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = searchEngineType;
                    d.Client = new ClientConfiguration() { MaxNumberOfRequestsPerSession = int.MaxValue };
                }
            };

            using (var store = GetDocumentStore(option))
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < size; ++i)
                    {
                        bulkInsert.Store(new Person() { Name = $"ItemNo{i}", Age = i % 100, Height = i % 200 });
                        expected.Add(new Result() { Age = i % 100, Height = i % 200 });
                    }
                }

                expected = expected.OrderBy(y => y.Age).ThenByDescending(y => y.Height).ToList();

                using (var session = store.OpenSession())
                {
                    List<Result> actual = new();
                    for (int i = 0; i < size; i += 70)
                        actual.AddRange(session.Query<Person>().OrderBy(y => y.Age).ThenByDescending(y => y.Height)
                            .Select(z => new Result() { Age = z.Age, Height = z.Height }).Skip(i).Take(70).ToList());

                    for (var i = 0; i < expected.Count; ++i)
                    {
                        Assert.Equal(expected[i].Age, actual[i].Age);
                        Assert.Equal(expected[i].Height, actual[i].Height);
                    }
                }
            }
        }

        [Theory]
        [SearchEngineClassData(SearchEngineType.Corax)]
        public void AnalyzerAreApplied(string searchEngineType)
        {
            var item = new Person() { Name = "MaCiEJ" };
            using var store = GetDocumentStore(Options.ForSearchEngine(searchEngineType));
            {
                using var session = store.OpenSession();
                session.Store(item);
                session.SaveChanges();
            }
            {
                using var session = store.OpenSession();
                var result = session.Query<Person>().Where(x => x.Name == "Maciej").SingleOrDefault();
                Assert.NotNull(result);
                Assert.Equal(item.Name, result.Name);
            }
        }

        private class Result
        {
            public int Age { get; set; }
            public int Height { get; set; }
        }

        private class Person
        {
            public string Name { get; set; }
            public int Age { get; set; }
            public int Height { get; set; }
        }
    }
}
