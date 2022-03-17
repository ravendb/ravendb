using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Dmitry : RavenTestBase
    {
        public Dmitry(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void DeepEqualsWorksWithTimeSpan()
        {
            var content = new MusicContent
            {
                Title = string.Format("Song # {0}", 1),
                Album = string.Format("Album # {0}", 1)
            };
            content.Keywords.Add("new");

            var obj = JToken.FromObject(content);
            var newObj = JToken.FromObject(content);

            Assert.True(JToken.DeepEquals(obj, newObj));
        }

        [Fact]
        public void TimeSpanWontTriggerPut()
        {
            using (var store = GetDocumentStore())
            {
                new MusicSearchIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    // Creating a big enough sample to reproduce
                    for (int i = 0; i < 100; i++)
                    {
                        var content = new MusicContent
                        {
                            Title = string.Format("Song # {0}", i + 1),
                            Album = string.Format("Album # {0}", (i % 8) + 1)
                        };

                        if (i > 0 && i % 10 == 0)
                        {
                            content.Keywords.Add("new");
                        }

                        session.Store(content);
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var content = session.Advanced
                        .DocumentQuery<MusicContent, MusicSearchIndex>()
                        .WhereLucene("Title", "<<new>>")
                        .WhereLucene("Album", "<<new>>")
                        .WhereLucene("Keywords", "<<new>>")
                        .Skip(1)
                        .Take(10)
                        .ToList();

                    Assert.False(session.Advanced.HasChanges);
                }
            }
        }

        private class MusicSearchIndex : AbstractIndexCreationTask<MusicContent, MusicSearchIndex.ReduceResult>
        {
            public class ReduceResult
            {
                public string Id { get; set; }
                public string Title { get; set; }
                public string Album { get; set; }
                public string[] Keywords { get; set; }

            }

            public MusicSearchIndex()
            {
                Map = results => from result in results
                                 select new
                                 {
                                     result.Id,
                                     Title = result.Title.Boost(10),
                                     result.Album,
                                     Keywords = result.Keywords.Boost(5)
                                 };

                Index(field => field.Title, FieldIndexing.Search);
                Index(field => field.Album, FieldIndexing.Search);
                Index(field => field.Keywords, FieldIndexing.Default);
            }
        }

        private abstract class Content
        {
            protected Content()
            {
                Keywords = new HashSet<string>();
            }

            public string Id { get; set; }
            public string Title { get; set; }
            public TimeSpan Duration { get; set; } // without this property it works

            public ICollection<string> Keywords { get; protected set; }
        }

        private class MusicContent : Content
        {
            public string Album { get; set; }
        }
    }
}
