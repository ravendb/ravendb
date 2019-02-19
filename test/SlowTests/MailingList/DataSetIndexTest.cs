using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class DataSetIndexTest : RavenTestBase
    {
        private const int MaxNumberOfItemsInDataSet = 50;

        [Fact]
        public void can_execute_query_default()
        {
            using (var store = GetDocumentStore())
            {
                new DataSetIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    CreateDataSet(session, "stations/rtl", "T1");
                    CreateDataSet(session, "stations/rtl", "T2");
                    CreateDataSet(session, "stations/energy", "EX");
                }

                // WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<DataSetIndex.Result, DataSetIndex>()
                        .WaitForNonStaleResults()
                        .AddOrder("Split_N1_D_Range", true)
                        .SelectFields<dynamic>("SongId", "Title", "Interpret", "Year", "Attributes", "SID", "SetId")
                        .Take(1024);
                    var result = query.ToList();
                    Assert.Equal("songs/50", result.First().SongId.ToString()); //GREEN
                }
            }
        }

        [Fact]
        public void can_execute_query_lazily()
        {
            using (var store = GetDocumentStore())
            {
                new DataSetIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    CreateDataSet(session, "stations/rtl", "T1");
                    CreateDataSet(session, "stations/rtl", "T2");
                    CreateDataSet(session, "stations/energy", "EX");
                }


                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<DataSetIndex.Result, DataSetIndex>()
                        .WaitForNonStaleResults()
                        .AddOrder("Split_N1_D_Range", true)
                        .SelectFields<dynamic>("SongId", "Title", "Interpret", "Year", "Attributes", "SID", "SetId")
                        .Take(1024);
                    var result = query.ToList();
                    Assert.Equal("songs/50", result.First().SongId.ToString()); //GREEN
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<DataSetIndex.Result, DataSetIndex>()
                                .WaitForNonStaleResults()
                                .AddOrder("Split_N1_D_Range", true)
                                .SelectFields<dynamic>("SongId", "Title", "Interpret", "Year", "Attributes", "SID", "SetId")
                                .Take(1024);
                    var result = query.Lazily().Value.ToList();
                    Assert.Equal("songs/50", result.First().SongId.ToString()); //RED! (:
                }
            }
        }

        private class DataSet
        {
            public string Id { get; set; }
            public List<Item> Items { get; set; }
            public string StationId { get; set; }
            public DateTime Date { get; set; }
        }

        private class Item
        {
            public List<Attribute> Attributes { get; set; }
            public string SongId { get; set; }
        }

        private class Attribute
        {
            public Attribute() { }
            public Attribute(string name, object value)
            {
                Name = name;
                Value = value;
            }
            public string Name { get; set; }
            public object Value { get; set; }
        }


        private static void CreateDataSet(IDocumentSession session, string stationId, string datasetKey)
        {
            var set = new DataSet
            {
                Id = stationId + "/test/" + datasetKey,
                StationId = stationId,
                Date = DateTime.UtcNow,
                Items = Enumerable.Range(1, MaxNumberOfItemsInDataSet).Select(x => new Item
                {
                    SongId = "songs/" + x,
                    Attributes = new[]
                        {
                            new Attribute("Split_N1", x*0.99d ),
                            new Attribute("Split_N4",x*0.01d),
                            new Attribute("SoundCode","Rock"),
                            new Attribute("Kat","T" + x)
                        }.ToList()
                }).ToList()
            };
            session.Store(set);
            session.SaveChanges();
        }

        private class DataSetIndex : AbstractIndexCreationTask<DataSet, DataSetIndex.Result>
        {
            public class Result
            {
                public string SetId { get; set; }
                public string SongId { get; set; }
                public string StationId { get; set; }
                public Attribute[] Attributes { get; set; }
                public DateTime Date { get; set; }
            }

            public DataSetIndex()
            {
                Map = sets =>
                      from set in sets
                      from item in set.Items
                      select new
                      {
                          SongId = item.SongId,
                          SetId = set.Id,
                          StationId = set.StationId,
                          Date = set.Date,
                          item.Attributes,
                          _ = "ignore"
                      };

                Reduce = results =>
                         from result in results
                         group result by new { result.SongId, result.StationId }
                             into g
                         select new
                         {
                             SongId = g.Key.SongId,
                             StationId = g.Key.StationId,
                             Date = g.OrderByDescending(x => x.Date).Select(x => x.Date).FirstOrDefault(),
                             SetId = g.OrderByDescending(x => x.Date).Select(x => x.SetId).FirstOrDefault(),
                             Attributes = g.OrderByDescending(x => x.Date).First().Attributes,
                             _ = g.OrderByDescending(x => x.Date).First().Attributes.Select(x => CreateField(x.Name, x.Value))
                         };

                Stores = new Dictionary<Expression<Func<Result, object>>, FieldStorage>()
                             {
                                 { e=>e.SongId, FieldStorage.Yes},
                                 { e=>e.SetId, FieldStorage.Yes},
                                 { e=>e.Attributes, FieldStorage.Yes},
                                 { e=>e.StationId, FieldStorage.Yes}
                             };
            }
        }

    }
}
