//-----------------------------------------------------------------------
// <copyright file="UsingDynamicQueryWithLocalServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Highlighting;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Querying
{
    public class TestClass : RavenTestBase
    {
        public TestClass(ITestOutputHelper output) : base(output)
        {
        }

        class TestObj
        {
            public Berufserfahrungen[] Berufserfahrungen { get; set; }
            public Tag[] Tags { get; set; }
        }
        class Berufserfahrungen
        {
            public string Tätigkeitsbeschreibung { get; set; }
        }
        class Tag
        {
            public string Tagname { get; set; }
        }

        public class Index_TESTIndexOff : AbstractIndexCreationTask
        {
            public class Result
            {
                public string KandidatenId { get; set; }
                public object[] Highlight { get; set; }
            }
            
            public override string IndexName => "TESTIndexOff";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"docs.TestObjs.Select(b => new {
   KandidatenId = Id(b),
    Highlight = new object[] {
        b.Name,
        b.Vorname,
         b.Tags.Select(e14 => e14.Tagname),
        b.Berufserfahrungen.Select(e16 => new {
            Tätigkeitsbeschreibung = e16.Tätigkeitsbeschreibung
        })
       
    }
})"
                    },
                    Reduce = @"results.Where(result => result.KandidatenId != null).GroupBy(result => result.KandidatenId).Select(g => new {
    g = g,
    b = DynamicEnumerable.FirstOrDefault(g)
}).Select(this1 => new {
    KandidatenId = this1.b.KandidatenId,
    Highlight = new object[] {
        this1.g.Select(e12 => e12.Highlight)
    }
   
})",
                    Fields =
                    {
                        { "Highlight", new IndexFieldOptions
                        {
                            Indexing = FieldIndexing.Search,
                            TermVector = FieldTermVector.WithPositionsAndOffsets } } ,
                        { "__all_fields", new IndexFieldOptions
                        {
                            Storage = FieldStorage.Yes } }
                    }
                };
            }
        }

        [Fact]
        public async Task TestCase()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new TestObj
                    {
                        Berufserfahrungen = new Berufserfahrungen[]
                        {
                            new Berufserfahrungen
                            {
                                Tätigkeitsbeschreibung = ", aa"
                            }
                        },
                        Tags = new Tag[]
                        {
                            new Tag{Tagname = "IT"}
                        }
                    });
                    await session.SaveChangesAsync();
                }

                await new Index_TESTIndexOff().ExecuteAsync(store);

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Advanced.AsyncDocumentQuery<Index_TESTIndexOff.Result, Index_TESTIndexOff>()
                        .WaitForNonStaleResults()
                        .Highlight("Highlight", 18, 1, out Highlightings titleHighlighting)
                        .Search("Highlight", "aa");
                    
                    var stringQuery = query.ToString();

                    WaitForUserToContinueTheTest(store);
                    _ = await query
                        .SingleAsync();

                }
            }
        }
    }

    public class HighlightesTests : RavenTestBase
    {
        public HighlightesTests(ITestOutputHelper output) : base(output)
        {
        }

        private interface ISearchable
        {
            string Slug { get; set; }
            string Title { get; set; }
            string Content { get; set; }
        }

        private class EventsItem : ISearchable
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public string Slug { get; set; }
            public string Content { get; set; }
        }

        private class SearchResults
        {
            public ISearchable Result { get; set; }
            public List<string> Highlights { get; set; }
            public string Title { get; set; }
        }

        private class ContentSearchIndex : AbstractMultiMapIndexCreationTask<ISearchable>
        {
            public ContentSearchIndex()
            {
                AddMap<EventsItem>(docs => from doc in docs
                                           let slug = doc.Id.ToString().Substring(doc.Id.ToString().IndexOf('/') + 1)
                                           select new { Slug = slug, doc.Title, doc.Content });


                Index(x => x.Slug, FieldIndexing.Search);
                Store(x => x.Slug, FieldStorage.Yes);
                TermVector(x => x.Slug, FieldTermVector.WithPositionsAndOffsets);

                Index(x => x.Title, FieldIndexing.Search);
                Store(x => x.Title, FieldStorage.Yes);
                TermVector(x => x.Title, FieldTermVector.WithPositionsAndOffsets);

                Index(x => x.Content, FieldIndexing.Search);
                Store(x => x.Content, FieldStorage.Yes);
                TermVector(x => x.Content, FieldTermVector.WithPositionsAndOffsets);
            }
        }

        [Theory]
        [InlineData("session")]
        public void SearchWithHighlightes(string q)
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new EventsItem
                    {
                        Slug = "ravendb-indexes-explained",
                        Title = "RavenDB indexes explained",
                        Content = "Itamar Syn-Hershko: Afraid of Map/Reduce? In this session, core RavenDB developer Itamar Syn-Hershko will walk through the RavenDB indexing process, grok it and much more.",
                    });
                    session.SaveChanges();
                }

                new ContentSearchIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    var options = new HighlightingOptions
                    {
                        PreTags = new[] { "<span style='background: yellow'>" },
                        PostTags = new[] { "</span>" }
                    };

                    var results = session.Advanced.DocumentQuery<ISearchable>("ContentSearchIndex")
                        .WaitForNonStaleResults()
                        .Highlight("Title", 128, 2, options, out Highlightings titleHighlighting)
                        .Highlight("Slug", 128, 2, options, out Highlightings slugHighlighting)
                        .Highlight("Content", 128, 2, options, out Highlightings contentHighlighting)
                        .Search("Slug", q).Boost(15)
                        .Search("Title", q).Boost(12)
                        .Search("Content", q)
                        .ToArray();

                    var orderedResults = new List<SearchResults>();

                    foreach (var searchable in results)
                    {

                        var docId = session.Advanced.GetDocumentId(searchable);

                        var highlights = new List<string>();
                        string title = null;
                        var titles = titleHighlighting.GetFragments(docId);
                        if (titles.Count() == 1)
                        {
                            title = titles[0];
                        }
                        else
                        {
                            highlights.AddRange(titleHighlighting.GetFragments(docId));
                        }
                        highlights.AddRange(slugHighlighting.GetFragments(docId));
                        highlights.AddRange(contentHighlighting.GetFragments(docId));


                        orderedResults.Add(new SearchResults { Result = searchable, Highlights = highlights, Title = title });
                    }
                }
            }
        }
    }
}
