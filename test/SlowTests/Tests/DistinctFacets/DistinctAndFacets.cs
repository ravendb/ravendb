// -----------------------------------------------------------------------
//  <copyright file="DistinctAndFacets.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.DistinctFacets
{
    public class DistinctAndFacets : RavenTestBase
    {
        public DistinctAndFacets(ITestOutputHelper output) : base(output)
        {
        }

        private class Book
        {
            public string Author;
            public string Category;
            public Tag[] Tags;
        }

        private class Tag
        {
            public string Name;
            public bool Primary;
        }

        private class Books_Search : AbstractIndexCreationTask<Book>
        {
            public Books_Search()
            {
                Map = books =>
                    from book in books
                    select new
                    {
                        book.Category,
                        book.Author,
                        PrimaryTag = book.Tags.Where(x => x.Primary).Select(x => x.Name),
                        SecondayTag = book.Tags.Where(x => x.Primary == false).Select(x => x.Name)
                    };
                Store(x => x.Author, FieldStorage.Yes);
            }

            public class Result
            {
#pragma warning disable 169,649
                public string Author;
#pragma warning restore 169,649
            }
        }

        private void SetupData(IDocumentStore store)
        {
            new Books_Search().Execute(store);
            using (var session = store.OpenSession())
            {
                session.Store(new Book
                {
                    Author = "authors/1",
                    Category = "Databases",
                    Tags = new Tag[]
                    {
                        new Tag {Name = "RavenDB", Primary = true},
                        new Tag {Name = "NoSQL", Primary = false}
                    }
                });
                session.Store(new Book
                {
                    Author = "authors/1",
                    Category = "Databases",
                    Tags = new Tag[]
                    {
                        new Tag {Name = "RavenDB", Primary = false},
                        new Tag {Name = "NoSQL", Primary = true}
                    }
                });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
        }

        [Fact]
        public void CanGetDistinctResult()
        {
            using (var store = GetDocumentStore())
            {
                SetupData(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.DocumentQuery<Book, Books_Search>()
                        .WhereEquals("PrimaryTag", "RavenDB").Boost(4)
                        .OrElse()
                        .WhereEquals("SecondayTag", "RavenDB").Boost(4)
                        .Distinct()
                        .SelectFields<Books_Search.Result>("Author")
                        .ToList();

                    Assert.Equal(1, results.Count);
                }
            }
        }

        [Fact]
        public void CanGetDistinctResult_WithPaging()
        {
            using (var store = GetDocumentStore())
            {
                SetupData(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.DocumentQuery<Book, Books_Search>()
                        .WhereEquals("PrimaryTag", "RavenDB").Boost(4)
                        .OrElse()
                        .WhereEquals("SecondayTag", "RavenDB").Boost(4)
                        .Distinct()
                        .SelectFields<Books_Search.Result>("Author")
                        .Skip(1)
                        .ToList();

                    Assert.Equal(0, results.Count);
                }
            }
        }

        [Fact]
        public void DistinctResult_WithFacets_ShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                SetupData(store);

                using (var session = store.OpenSession())
                {
                    var ex = Assert.Throws<InvalidOperationException>(() =>
                    {
                        session.Advanced.DocumentQuery<Book, Books_Search>()
                            .WhereEquals("PrimaryTag", "RavenDB").Boost(4)
                            .OrElse()
                            .WhereEquals("SecondayTag", "RavenDB").Boost(4)
                            .Distinct()
                            .SelectFields<Books_Search.Result>("Author")
                            .AggregateBy(x => x.ByField("Category"))
                            .Execute();
                    });

                    Assert.Equal("Aggregation query can select only facets while it got DistinctToken token", ex.Message);
                }
            }
        }
    }
}
