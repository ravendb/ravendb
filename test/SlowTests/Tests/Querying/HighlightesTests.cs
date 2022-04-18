//-----------------------------------------------------------------------
// <copyright file="UsingDynamicQueryWithLocalServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Highlighting;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Querying
{
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
        [RavenData("session", SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void SearchWithHighlightes(Options options, string q)
        {
            using (var store = GetDocumentStore(options))
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
                    var highlightingOptions = new HighlightingOptions
                    {
                        PreTags = new[] { "<span style='background: yellow'>" },
                        PostTags = new[] { "</span>" }
                    };

                    var results = session.Advanced.DocumentQuery<ISearchable>("ContentSearchIndex")
                        .WaitForNonStaleResults()
                        .Highlight("Title", 128, 2, highlightingOptions, out Highlightings titleHighlighting)
                        .Highlight("Slug", 128, 2, highlightingOptions, out Highlightings slugHighlighting)
                        .Highlight("Content", 128, 2, highlightingOptions, out Highlightings contentHighlighting)
                        .Search("Slug", q).Boost(15)
                        .Search("Title", q).Boost(12)
                        .Search("Content", q)
                        .ToArray();

                    Assert.Equal(1, results.Length);

                    var docId = session.Advanced.GetDocumentId(results.First());

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

                    Assert.Equal(1, highlights.Count);                    
                }
            }
        }
    }
}
