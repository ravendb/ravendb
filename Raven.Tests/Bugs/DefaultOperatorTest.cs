// -----------------------------------------------------------------------
//  <copyright file="DefaultOperatorTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
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

        private EmbeddableDocumentStore SetupSampleData()
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

            var store = NewDocumentStore();
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

        // This test passes
        [Fact]
        public void ShouldRespectDefaultOperator()
        {
            using (var store = SetupSampleData())
            {
                using (var s = store.OpenSession())
                {
                    var result = s.Advanced.DocumentQuery<Blog, BlogIndex>()
                            .Search(x => x.Title, "one two").UsingDefaultOperator(Raven.Abstractions.Data.QueryOperator.And)
                            .WaitForNonStaleResultsAsOfNow(TimeSpan.FromSeconds(5))
                            .ToList();

                    Assert.True(result.Count == 1);
                }
            }
        }

        // This test fails
        [Fact]
        public void ShouldRespectDefaultOperatorCombinedWithSelectFields()
        {
            using (var store = SetupSampleData())
            {
                using (var s = store.OpenSession())
                {
                    var result = s.Advanced.DocumentQuery<Blog, BlogIndex>()
                            .Search(x => x.Title, "one two").UsingDefaultOperator(Raven.Abstractions.Data.QueryOperator.And)
                            .SelectFields<Blog>("Title")
                            .WaitForNonStaleResultsAsOfNow(TimeSpan.FromSeconds(5))
                            .ToList();

                    Assert.True(result.Count == 1);
                }
            }
        }

        public class BlogIndex : AbstractIndexCreationTask<BlogIndex.Blog>
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
    }
}