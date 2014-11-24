// -----------------------------------------------------------------------
//  <copyright file="AutoGenIndexLinqQuery.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class AutoGenIndexLinqQuery : RavenTest
    {
        public class Book
        {
            public String Name { get; set; }

            public List<BookPost> Posts { get; set; }
        }

        public class BookPost
        {
            public string Title { get; set; }

            public BookPostType? Type { get; set; }

            public enum BookPostType
            {
                BooPost1,
                BooPost2,
                BooPost3
            }
        }
  
        [Fact]
        public void ShouldWork()
        {
            using (var documentStore = NewDocumentStore())
            {
                var bookName = "Book";
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Book
                    {
                        Name = bookName,
                        Posts = new List<BookPost> {new BookPost()
                                    {
                                        Title = "A post",
                                        Type = BookPost.BookPostType.BooPost1
                                    }
                                }
                    });

                    session.Store(new Book
                    {
                        Name = bookName,
                        Posts = new List<BookPost> {new BookPost()
                                    {
                                        Title = "A post",
                                        Type = BookPost.BookPostType.BooPost2
                                    }
                                }
                    });
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var ravenQueryable = session.Query<Book>().Customize(b => b.WaitForNonStaleResultsAsOfLastWrite()).ToList();
                }

                using (var session = documentStore.OpenSession())
                {
                    var bookToGet = new List<string>() { bookName };
                    var bookPostToGet = new List<BookPost.BookPostType?> { BookPost.BookPostType.BooPost1 };
                    var books = session.Query<Book>().Where(b => b.Name.In(bookToGet));
                    books = books.Where(b => b.Posts.Any(p => p.Type.In(bookPostToGet)));

                    var bookPage = books.ToList();

                }
            }
            
        }
    }
}