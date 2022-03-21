using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_1489 : RavenTestBase
    {
        public RavenDB_1489(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Querying_index_with_condtional_count_should_work()
        {
            var dataEntries = GenerateDataEntries();

            using (var documentStore = GetDocumentStore())
            {
                new MapReduceIndexWithCountAndCondition().Execute(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    foreach (var entry in dataEntries)
                        session.Store(entry);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var fetchedIndexedDocuments = session.Query<IndexEntry, MapReduceIndexWithCountAndCondition>().ToList();

                    Assert.NotEmpty(fetchedIndexedDocuments);
                }
            }
        }

        public class DataEntry
        {
            public string Id { get; set; }

            public int Property1 { get; set; }

            public IEnumerable<Tuple<int, int>> Property2 { get; set; }
        }

        public class IndexEntry
        {
            public int Property1 { get; set; }

            public int EntryCountWithPositiveValue { get; set; }

            public int TotalCount { get; set; }
        }

        public class MapReduceIndexWithCountAndCondition : AbstractIndexCreationTask<DataEntry, IndexEntry>
        {
            public MapReduceIndexWithCountAndCondition()
            {
                Map = dataEntries => from entry in dataEntries
                                     let numbersMapping = from tuple in entry.Property2
                                                          select new
                                                          {
                                                              P1 = entry.Property1,
                                                              Key = tuple.Item1,
                                                              Value = tuple.Item2
                                                          }

                                     from numberMap in numbersMapping
                                     group numberMap by new { numberMap.P1 }
                    into g
                                     select new
                                     {
                                         Property1 = g.Key.P1,
                                         EntryCountWithPositiveValue = g.Count(row => row.Value > 0),//g.Where(row => row.Value > 0).Count(),
                                         TotalCount = 1
                                     };

                Reduce = results => from result in results
                                    group result by result.Property1
                    into g
                                    select new
                                    {
                                        Property1 = g.Key,
                                        EntryCountWithPositiveValue = g.Max(row => row.EntryCountWithPositiveValue),
                                        TotalCount = g.Sum(row => row.TotalCount)
                                    };

                StoreAllFields(FieldStorage.No);
            }
        }

        private IEnumerable<DataEntry> GenerateDataEntries()
        {
            for (int i = 0; i < 10; i++)
            {
                if (i % 2 == 0)
                {
                    var dataEntry = new DataEntry()
                    {
                        Property1 = i,
                        Property2 = new[]
                        {
                            Tuple.Create(-1,2),
                            Tuple.Create(-1,-1),
                            Tuple.Create(0,0),
                            Tuple.Create(0,1),
                            Tuple.Create(1,2),
                            Tuple.Create(1,3),
                            Tuple.Create(2,4),
                            Tuple.Create(2,5),
                            Tuple.Create(-2,5),
                        }
                    };

                    yield return dataEntry;
                }
                else
                {
                    var dataEntry = new DataEntry()
                    {
                        Property1 = i,
                        Property2 = new[]
                        {
                            Tuple.Create(0,0),
                            Tuple.Create(0,-1),
                            Tuple.Create(1,-2),
                            Tuple.Create(1,-3),
                            Tuple.Create(2,-4),
                            Tuple.Create(2,-5),
                        }
                    };

                    yield return dataEntry;
                }
            }
        }
    }
}
