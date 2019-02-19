// -----------------------------------------------------------------------
//  <copyright file="Arun.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class Arun : RavenTestBase
    {
        [Fact]
        public void SaveDynamicEntityWithStronglyTypedProperties()
        {
            QueryStatistics stats = null;
            using (var store = GetDocumentStore())
            {
                new BookSearch().Execute(store);

                Book book = new Book()
                {
                    Title = "Greatest Ever",
                    Publisher = "DC",
                    Contributors = new List<Contributor>()
                    {
                        new Contributor() {Name = "Jack Sparrow"},
                        new Contributor() {Name = "Will Smith"},
                        new Contributor() {Name = "Wayne Rooney"}
                    },
                    Prices = new List<Price>()
                    {
                        new Price() {price = "10$"},
                        new Price() {price = "8£"}
                    },
                    Subjects = new List<Subject>()
                    {
                        new Subject() {Code = "1TGS"},
                        new Subject() {Code = "8TRD"}
                    }
                };

                using (var session = store.OpenSession())
                {
                    session.Store(book);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    string[] searchTerms = { "Will", "Wayne", "Jack" };


                    var ravenQueryable = session.Query<Book, BookSearch>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Keywords.In(searchTerms));

                    var bookinfo = ravenQueryable
                        .Statistics(out stats)
                        .ToArray();
                }
            }

            Assert.Equal(1, stats.TotalResults);
        }

        private class Book
        {
            public string Keywords { get; set; }


            [DataMember]
            public string Title { get; set; }

            [DataMember]
            public List<Subject> Subjects { get; set; }

            [DataMember]
            public List<Contributor> Contributors { get; set; }

            [DataMember]
            public List<Tag> Tags { get; set; }


            [DataMember]
            public string Publisher { get; set; }

            [DataMember]
            public List<Price> Prices { get; set; }
        }

        public class Subject
        {
            public string Code { get; set; }
        }

        private class Contributor
        {
            public string Name { get; set; }
        }

        private class Price
        {
            public string price { get; set; }
        }

        private class Tag
        {
            public string Name { get; set; }
        }

        private class BookSearch : AbstractIndexCreationTask<Book, BookSearch.Result>
        {
            public BookSearch()
            {
                Map = books => from book in books
                               select new
                               {
                                   Keywords = new object[]
                                {
                                    book.Title,
                                    book.Contributors.Select(contributor => contributor.Name),
                                    book.Tags.Select(tag => tag.Name),
                                    book.Subjects.Select(subject => subject.Code)
                                },
                                   Publisher = book.Publisher,
                                   Prices_CountryCodes = book.Prices.Select(price => price.price)
                               };
                Analyzers.Add(book => book.Keywords, "Lucene.Net.Analysis.Standard.StandardAnalyzer");
                Index(book => book.Keywords, FieldIndexing.Search);
            }

            public class Result
            {
                /// <summary>
                /// Gets or sets the keywords for the index for searching the books.
                /// </summary>
                public object[] Keywords { get; set; }


            }
        }
    }
}
