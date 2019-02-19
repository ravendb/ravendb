using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class ItemsBySetIdIndexTests : RavenTestBase
    {
        [Fact]
        public void CanQuery()
        {
            using (var store = GetDocumentStore())
            {
                new ItemsBySetIdIndex().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Station { Id = "stations/radiofm" });
                    session.Store(new Song { Id = "songs/adrianstern", Title = "AMERIKA", Interpret = "ADRIAN STERN" });
                    session.Store(new Song { Id = "songs/falco", Title = "EGOIST", Interpret = "FALCO" });
                    session.Store(new SongConfig { Id = "stations/radiofm/songs/adrianstern", Attributes = new[] { new Attribute { Name = "SoundCode", Value = "POP" } } });
                    session.Store(new SongConfig { Id = "stations/radiofm/songs/falco", Attributes = new[] { new Attribute { Name = "SoundCode", Value = "ROCK" } } });
                    session.Store(new Item //TEST-DATA
                    {
                        Id = "stations/radiofm/tests/001/songs/adrianstern",
                        Set = "stations/radiofm/tests/001", //ref to the dataset
                        StationId = "stations/radiofm", //ref to the station
                        RelatedTo = "stations/radiofm/songs/adrianstern", //ref to the global.config
                        SongId = "songs/adrianstern", //ref to the song
                        Attributes = new[] { new Attribute { Name = "TixN", Value = 1.5m } }
                    });
                    session.Store(new Item //TEST-DATA
                    {
                        Id = "stations/radiofm/tests/001/songs/falco",
                        Set = "stations/radiofm/tests/001",
                        StationId = "stations/radiofm",
                        RelatedTo = "stations/radiofm/songs/falco",
                        SongId = "songs/falco",
                        Attributes = new[] { new Attribute { Name = "TixN", Value = 3.5m } }
                    });
                    session.SaveChanges();
                }
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<ItemsBySetIdIndex.Result, ItemsBySetIdIndex>()
                                .WhereEquals("SetId", "stations/radiofm/tests/001")
                                .AndAlso()
                                .WhereEquals("SoundCode", "ROCK")
                                .SelectFields<DataView>("SongId", "Title", "Interpret", "Year", "Attributes", "SID", "SetId", "NumberOfTests", "LastTestDate", "LastTestId", "Date");
                    var total = query.GetQueryResult().TotalResults;
                    Assert.Equal(1, total);
                    Assert.Equal("EGOIST", query.First().Title);
                }
            }

        }

        private class Station
        {
            public string Id { get; set; }
        }

        private class Item
        {
            public string SongId { get; set; }
            public string RelatedTo { get; set; }
            public IList<Attribute> Attributes { get; set; }
            public string Set { get; set; }
            public string StationId { get; set; }
            public DateTime Date { get; set; }
            public bool IsRotation { get; set; }
            public string Id { get; set; }
        }

        private class Attribute
        {
            public string Name { get; set; }
            public object Value { get; set; }
        }

        private class Song
        {
            public string Id { get; set; }
            public string Interpret { get; set; }
            public string Title { get; set; }
            public int Year { get; set; }
        }

        private class SongConfig
        {
            public IList<Attribute> Attributes { get; set; }
            public int NumberOfTests { get; set; }
            public string LastTestId { get; set; }
            public DateTime LastTestDate { get; set; }
            public string Id { get; set; }
        }

        private class DataView
        {
            public string SongId { get; set; }
            public string Title { get; set; }
            public string Interpret { get; set; }
            public int Year { get; set; }
            public string SID { get; set; }
            public string SetId { get; set; }
            public string StationId { get; set; }
            public DateTime? Date { get; set; }
            public List<Attribute> Attributes { get; set; }
            public int? NumberOfTests { get; set; }
            public DateTime? LastTestDate { get; set; }
            public string LastTestId { get; set; }
        }

        private class ItemsBySetIdIndex : AbstractIndexCreationTask<Item, ItemsBySetIdIndex.Result>
        {
            public class Result
            {
                public string SetId { get; set; }
                public string SongId { get; set; }
                public string Title { get; set; }
                public string Interpret { get; set; }
                public int Year { get; set; }
                public string StationId { get; set; }
                public Attribute[] Attributes { get; set; }
                public DateTime Date { get; set; }
                public bool IsRotation { get; set; }
                public string Title_Sort { get; set; }
                public string Interpret_Sort { get; set; }
                public string LastTestId { get; set; }
                public DateTime? LastTestDate { get; set; }
                public int NumberOfTests { get; set; }
            }

            public ItemsBySetIdIndex()
            {
                Map = items => from item in items
                               let song = LoadDocument<Song>(item.SongId)
                               let config = LoadDocument<SongConfig>(item.RelatedTo)
                               let globalAttributes = config.Attributes
                               let attributes = item.Attributes.Union(globalAttributes)
                               select new
                               {
                                   SetId = item.Set,
                                   StationId = item.StationId,
                                   Date = item.Date,
                                   IsRotation = item.IsRotation,
                                   SongId = song.Id,
                                   Interpret = song.Interpret,
                                   Title = song.Title,
                                   Interpret_Sort = song.Interpret,
                                   Title_Sort = song.Title,
                                   Year = song.Year,
                                   Attributes = attributes,
                                   NumberOfTests = config.NumberOfTests,
                                   LastTestId = config.LastTestId,
                                   LastTestDate = config.LastTestDate,
                                   _ = attributes.Select(x => CreateField(x.Name, x.Value)),
                                   //but it will work!:
                                   //_ = item.Attributes.Select(x => CreateField(x.Name, x.Value)),
                                   //__ = globalAttributes.Select(x => CreateField(x.Name, x.Value))
                               };


                Index(x => x.Interpret, FieldIndexing.Search);
                Index(x => x.Title, FieldIndexing.Search);

                Stores = new Dictionary<Expression<Func<Result, object>>, FieldStorage>()
                {
                    { e=>e.SongId, FieldStorage.Yes},
                    { e=>e.SetId, FieldStorage.Yes},
                    { e=>e.Date, FieldStorage.Yes},
                    { e=>e.Title, FieldStorage.Yes},
                    { e=>e.Interpret, FieldStorage.Yes},
                    { e=>e.Year, FieldStorage.Yes},
                    { e=>e.Attributes, FieldStorage.Yes},
                    { e=>e.StationId, FieldStorage.Yes},
                    { e=>e.NumberOfTests, FieldStorage.Yes},
                    { e=>e.LastTestDate, FieldStorage.Yes},
                    { e=>e.LastTestId, FieldStorage.Yes}
                };
            }
        }

    }
}
