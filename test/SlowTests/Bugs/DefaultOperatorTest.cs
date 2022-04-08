// -----------------------------------------------------------------------
//  <copyright file="DefaultOperatorTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class DefaultOperatorTest : RavenTestBase
    {
        public DefaultOperatorTest(ITestOutputHelper output) : base(output)
        {
        }

        private class Blog
        {
            public string Title { get; set; }
            public string Category { get; set; }
            public BlogTag[] Tags { get; set; }
        }

        private class BlogTag
        {
            public string Name { get; set; }
        }

        private DocumentStore SetupSampleData(Options options)
        {
            var blogOne = new Blog
            {
                Title = "one two three",
                Category = "Ravens",
                Tags = new[]
                                {
                                        new BlogTag {Name = "Birds"}
                                }
            };
            var blogTwo = new Blog
            {
                Title = "two three four",
                Category = "Rhinos",
                Tags = new[]
                                {
                                        new BlogTag {Name = "Mammals"}
                                }
            };
            var blogThree = new Blog
            {
                Title = "three four five",
                Category = "Rhinos",
                Tags = new[]
                                {
                                        new BlogTag {Name = "Mammals"}
                                }
            };

            var store = GetDocumentStore(options);
            using (var s = store.OpenSession())
            {
                s.Store(blogOne);
                s.Store(blogTwo);
                s.Store(blogThree);
                s.SaveChanges();
            }
            new BlogIndex().Execute(store);

            return store;
        }

        private class BlogIndex : AbstractIndexCreationTask<BlogIndex.Blog>
        {
            public class Blog
            {
                public string Title { get; set; }
                public string Category { get; set; }
                public BlogTag[] Tags { get; set; }
            }

            public class BlogTag
            {
                public string Name { get; set; }

            }
            public BlogIndex()
            {
                Map = docs => from doc in docs
                              select new Blog
                              {
                                  Title = doc.Title,
                                  Tags = doc.Tags
                              };

                Stores.Add(i => i.Tags, FieldStorage.Yes);
                Index(i => i.Tags, FieldIndexing.No);
                Analyze(x => x.Title, "Lucene.Net.Analysis.Standard.StandardAnalyzer, Lucene.Net");
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ShouldRespectDefaultOperator(Options options)
        {
            using (var store = SetupSampleData(options))
            {
                using (var s = store.OpenSession())
                {
                    var result = s.Advanced.DocumentQuery<Blog, BlogIndex>()
                        .Search(x => x.Title, "one two", SearchOperator.And)
                        .WaitForNonStaleResults(TimeSpan.FromSeconds(5))
                        .ToList();

                    Assert.True(result.Count == 1);
                }
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ShouldRespectDefaultOperatorCombinedWithSelectFields(Options options)
        {
            using (var store = SetupSampleData(options))
            {
                using (var s = store.OpenSession())
                {
                    var result = s.Advanced.DocumentQuery<Blog, BlogIndex>()
                        .Search(x => x.Title, "one two", SearchOperator.And)
                        .SelectFields<Blog>("Title")
                        .WaitForNonStaleResults(TimeSpan.FromSeconds(5))
                        .ToList();

                    Assert.True(result.Count == 1);
                }
            }
        }

    }
}
