// -----------------------------------------------------------------------
//  <copyright file="FragementFail.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Highlighting;
using Xunit;

namespace SlowTests.MailingList
{
    public class FragmentFail : RavenTestBase
    {
        private class TheArticle
        {
            public string Id { get; set; }
            public string Heading { get; set; }
            public string MainIntro { get; set; }
            public string MainBody { get; set; }
        }

        private class The_Search : AbstractIndexCreationTask<TheArticle, The_Search.Result>
        {
            public class Result
            {
                public string Query { get; set; }
            }

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"from doc in docs
                        select new { Query = new [] { doc.MainIntro, doc.MainBody, doc.Heading } }"
                    },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        {
                            "Query", new IndexFieldOptions
                            {
                                Storage = FieldStorage.Yes,
                                TermVector = FieldTermVector.WithPositionsAndOffsets,
                                Indexing = FieldIndexing.Search
                            }
                        }
                    }
                };
            }
        }

        [Fact]
        public void Fragment_Length_Should_Not_Be_More_Then_128()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TheArticle
                    {
                        Id = "articles/1",
                        Heading = "brickpile kicks ass",
                        MainIntro = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. In et tortor vulputate, congue felis non, iaculis erat. Maecenas pellentesque, nisl at blandit posuere, velit turpis vulputate tortor, tempor pellentesque nisl enim sit amet mi.",
                        MainBody = "<p>Knark</p>\r\n<p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. In et tortor vulputate, congue felis non, iaculis erat. Maecenas pellentesque, nisl at blandit posuere, velit turpis vulputate tortor, tempor pellentesque nisl enim sit amet mi. Duis scelerisque metus in massa dictum, quis varius libero malesuada. Maecenas semper vitae massa et auctor. Sed tortor est, pharetra et venenatis eu, viverra vel lacus. Quisque iaculis ligula id urna bibendum, efficitur vehicula felis gravida. Quisque nec tempor nisl, nec aliquet libero. In quis ultrices nisi, in fermentum arcu. Aenean vel pretium risus, nec congue dui. Vestibulum viverra maximus tristique. Cras eu nibh a enim vehicula laoreet. Morbi non suscipit mauris, et tincidunt lectus. Suspendisse felis elit, aliquam non vulputate a, sollicitudin vel ante. Cras nec urna ex.</p>\r\n<p>Fusce tincidunt, velit quis condimentum ullamcorper, lorem nulla interdum tellus, nec luctus lectus neque maximus turpis. Vestibulum dapibus est eget rhoncus molestie. Vestibulum ac sem sapien. Donec sodales justo eu diam elementum porttitor. Etiam posuere lorem in ornare molestie. Quisque sagittis eros id rhoncus ornare. Etiam ullamcorper nibh at urna porta, vitae ultricies urna facilisis. Donec semper sapien nec convallis fringilla. Maecenas a vulputate dolor. Donec elementum cursus lorem, sit amet tristique risus euismod et. Phasellus tincidunt quam felis, sit amet pretium lacus dapibus in. Etiam maximus varius dui, eget euismod ante fermentum ac. Aliquam iaculis varius sapien, rutrum aliquam nisi cursus ut. Nulla nec volutpat tellus.</p>\r\n<p>Donec porta, odio id finibus tristique, velit mauris mollis lacus, quis posuere ligula magna sed enim. Cras in fringilla orci. Nunc imperdiet hendrerit felis non fringilla. Vestibulum feugiat odio ac quam congue consectetur. Nunc iaculis mi dui, ut varius lorem gravida in. Fusce ac rhoncus nisl, a luctus magna. Donec et vehicula turpis. Duis consectetur eleifend tellus, in pulvinar arcu viverra at. In quis rhoncus risus. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam sit amet auctor nunc, nec scelerisque elit. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Nam nec erat tristique, lacinia ipsum porttitor, dictum nibh. Integer ut nulla a mauris scelerisque bibendum sed vitae tellus.</p>\r\n<p>Nullam ultrices dapibus sollicitudin. Nullam placerat, risus non convallis blandit, lacus diam fermentum eros, vel dapibus ex nisi et mi. Interdum et malesuada fames ac ante ipsum primis in faucibus. Duis pellentesque sagittis mattis. Fusce vel vestibulum mi. Sed ac turpis vitae urna imperdiet commodo vel vitae lacus. Maecenas vel semper tellus. Nunc tempus massa elit, ut sollicitudin leo aliquet et. Nulla tristique, tellus ut venenatis varius, arcu ligula luctus magna, id condimentum justo turpis eget nisi. Nullam et justo eget nulla sollicitudin rutrum. Aenean libero felis, hendrerit hendrerit diam nec, commodo bibendum ligula. Suspendisse id velit maximus, tincidunt dui quis, eleifend dolor. In dignissim sem quis leo venenatis, eget laoreet nibh lacinia. In id eros felis. Nullam cursus, ipsum eu semper volutpat, metus quam pulvinar metus, ut commodo lectus est non ipsum. Nullam consectetur turpis vulputate justo sollicitudin, eu tempor velit maximus.</p>\r\n<p>Curabitur lobortis rutrum dolor vitae semper. Praesent congue dolor et nisl commodo rutrum. Vestibulum malesuada sem odio, sed posuere nisl accumsan ac. Vestibulum vehicula, odio vitae placerat tristique, nisi massa rhoncus ante, at porttitor tellus nisl a lorem. Praesent pellentesque, urna sit amet ornare faucibus, risus tortor scelerisque nibh, eu sollicitudin tellus nibh sed nisl. Sed nec ipsum sem. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Etiam a ullamcorper erat. Nulla at porttitor ligula. Suspendisse eget ex bibendum, euismod dolor vel, sodales massa. Donec tempus maximus lacinia. Proin finibus quam vel ullamcorper semper. Duis nunc lectus, laoreet ut condimentum sed, pulvinar lacinia libero.</p>"
                    });
                    session.SaveChanges();
                }

                new The_Search().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    string q = "brickpile";
                    Highlightings h = null;
                    var hits = session.Query<The_Search.Result, The_Search>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Highlight(x => x.Query, 128, 1, out h)
                        .Search(x => x.Query, q)
                        .As<TheArticle>()
                        .ToList();

                    var fragment = h.GetFragments("articles/1").First();
                    Assert.True(fragment.Length < 128);
                }
            }
        }
    }
}
