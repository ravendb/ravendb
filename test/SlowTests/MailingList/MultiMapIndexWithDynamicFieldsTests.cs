using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class MultiMapIndexWithDynamicFieldsTests : RavenTestBase
    {
        [Fact]
        public void CanSortDynamically()
        {
            using (var store = GetDocumentStore())
            {
                new DynamicMultiMapDataSetIndex().Execute(store);
                using (var session = store.OpenSession())
                {
                    foreach (var x in Enumerable.Range(1, 50))
                    {
                        session.Store(new Song
                        {
                            Id = "songs/" + x,
                            Title = "Title:" + x,
                            Interpret = "Interpret: " + x,
                            Year = 2012,
                            Attributes = new[] { new Attribute("SoundCode", "Rock") }
                        });
                    }

                    session.Store(new DataSet
                    {
                        Items = Enumerable.Range(1, 50).Select(x =>
                        new Item
                        {
                            SongId = "songs/" + x,
                            Attributes = new[]
                        {
                            new Attribute("N1",x*0.99d),
                            new Attribute("N4",x*0.01d)
                        }
                        }).ToList()
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.DocumentQuery<DynamicMultiMapDataSetIndex.Result, DynamicMultiMapDataSetIndex>()
                        .WaitForNonStaleResults()
                        .AddOrder("N1_D_Range", true)
                        .Take(128)
                        .ToList();
                    Assert.Equal(50, result.Count); //FAIL(:
                    Assert.Equal((decimal)49.50, result.First().Attributes.First(x => x.Name == "N1").Value);
                }
            }
        }

        private class Song
        {
            public string Id { get; set; }

            public string Title { get; set; }

            public string Interpret { get; set; }

            public int Year { get; set; }

            public Attribute[] Attributes { get; set; }
        }

        private class DataSet
        {
            public string Id { get; set; }
            public List<Item> Items { get; set; }
        }

        private class Item
        {
            public Attribute[] Attributes { get; set; }
            public string SongId { get; set; }
        }

        private class Attribute
        {
            protected Attribute() { }
            public Attribute(string name, object value)
            {
                Name = name;
                Value = value;
            }
            public string Name { get; set; }
            public object Value { get; set; }
        }

        private class DataView
        {
            public string SongId { get; set; }

            public string Title { get; set; }

            public string Interpret { get; set; }

            public int Year { get; set; }

            public Attribute[] Attributes { get; set; }
        }

        private class DynamicMultiMapDataSetIndex : AbstractMultiMapIndexCreationTask<DynamicMultiMapDataSetIndex.Result>
        {
            public class Result
            {
                public string SetId { get; set; }
                public string SongId { get; set; }
                public string Title { get; set; }
                public string Interpret { get; set; }
                public int Year { get; set; }
                public Attribute[] Attributes { get; set; }
            }

            public DynamicMultiMapDataSetIndex()
            {
                AddMap<DataSet>(sets => from dataSet in sets
                                        from item in dataSet.Items
                                        select new
                                        {
                                            SetId = dataSet.Id,
                                            SongId = item.SongId,
                                            Title = (string)null,
                                            Interpret = (string)null,
                                            Year = -1,
                                            Attributes = item.Attributes,
                                            _ = "ignored"
                                        });
                AddMap<Song>(songs => from song in songs
                                      select new
                                      {
                                          SetId = (string)null,
                                          SongId = song.Id,
                                          Title = song.Title,
                                          Interpret = song.Interpret,
                                          Year = song.Year,
                                          Attributes = song.Attributes,
                                          _ = "ignored"
                                      });


                Reduce = results => from result in results
                                    group result by result.SongId into g
                                    select new
                                    {
                                        SetId = g.Select(x => x.SetId).FirstOrDefault(x => x != null),
                                        SongId = g.Key,
                                        Title = g.Select(x => x.Title).FirstOrDefault(x => x != null),
                                        Interpret = g.Select(x => x.Interpret).FirstOrDefault(x => x != null),
                                        Year = g.Select(x => x.Year).FirstOrDefault(x => x >= 0),
                                        Attributes = g.SelectMany(x => x.Attributes).Where(x => x != null),
                                        _ = g.SelectMany(x => x.Attributes).Select(x => CreateField(x.Name, x.Value, true, true))
                                    };
            }
        }

    }
}
