using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Highlighting;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6558 : RavenTestBase
    {
        public RavenDB_6558(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Highlighting)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanUseDifferentPreAndPostTagsPerField(Options options)
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
                    }, "items/1");
                    session.SaveChanges();
                }

                new ContentSearchIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    var options1 = new HighlightingOptions
                    {
                        PreTags = new[] { "***" },
                        PostTags = new[] { "***" }
                    };

                    var options2 = new HighlightingOptions
                    {
                        PreTags = new[] { "^^^" },
                        PostTags = new[] { "^^^" }
                    };

                    var results = session.Advanced.DocumentQuery<ISearchable>("ContentSearchIndex")
                        .WaitForNonStaleResults()
                        .Highlight("Title", 128, 2, options1, out Highlightings titleHighlighting)
                        .Highlight("Content", 128, 2, options2, out Highlightings contentHighlighting)
                        .Search("Title", "RavenDB").Boost(12)
                        .Search("Content", "RavenDB")
                        .ToArray();

                    Assert.Contains("***", titleHighlighting.GetFragments("items/1").First());
                    Assert.Contains("^^^", contentHighlighting.GetFragments("items/1").First());
                }
            }
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
    }
}
