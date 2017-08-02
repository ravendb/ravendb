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
using Xunit;

namespace SlowTests.Bugs
{
    public class DefaultOperatorTest : RavenTestBase
    {
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

        private DocumentStore SetupSampleData()
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

            var store = GetDocumentStore();
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

                Analyze(x => x.Title, "Lucene.Net.Analysis.Standard.StandardAnalyzer, Lucene.Net");
            }
        }

        [Fact]
        public void ShouldRespectDefaultOperator()
        {
            using (var store = SetupSampleData())
            {
                using (var s = store.OpenSession())
                {
                    var result = s.Advanced.DocumentQuery<Blog, BlogIndex>()
                        .Search(x => x.Title, "one two", SearchOperator.And)
                        .WaitForNonStaleResultsAsOfNow(TimeSpan.FromSeconds(5))
                        .ToList();

                    Assert.True(result.Count == 1);
                }
            }
        }

        [Fact]
        public void ShouldRespectDefaultOperatorCombinedWithSelectFields()
        {
            using (var store = SetupSampleData())
            {
                using (var s = store.OpenSession())
                {
                    var result = s.Advanced.DocumentQuery<Blog, BlogIndex>()
                        .Search(x => x.Title, "one two", SearchOperator.And)
                        .SelectFields<Blog>("Title")
                        .WaitForNonStaleResultsAsOfNow(TimeSpan.FromSeconds(5))
                        .ToList();

                    Assert.True(result.Count == 1);
                }
            }
        }

    }
}
