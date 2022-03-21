// -----------------------------------------------------------------------
//  <copyright file="LazyEncoding.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class LazyEncoding : RavenTestBase
    {
        public LazyEncoding(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name { get; set; }
            public bool Admin { get; set; }
        }

        private class UserIndex : AbstractIndexCreationTask<User>
        {
            public UserIndex()
            {
                Map = users =>
                    from u in users
                    select new { u.Name, u.Admin };
            }
        }

        [Fact]
        public void ShouldNotMatterForFacets()
        {
            using (var store = GetDocumentStore())
            {
                new UserIndex().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Admin = true
                    });

                    session.SaveChanges();
                }
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var x = session.Advanced.DocumentQuery<User, UserIndex>()
                        .WhereLucene("Name", "Oren")
                        .AndAlso()
                        .WhereLucene("Name", "Eini")
                        .AggregateBy(new Facet[]
                        {
                            new Facet<User>
                            {
                                FieldName = user => user.Admin
                            }
                        })
                        .Execute();

                    Assert.Empty(x["Admin"].Values);
                }

                using (var session = store.OpenSession())
                {
                    var x = session.Advanced.DocumentQuery<User, UserIndex>()
                        .WhereLucene("Name", "Oren")
                        .AndAlso()
                        .WhereLucene("Name", "Eini")
                        .AggregateBy(new Facet[]
                        {
                            new Facet<User>
                            {
                                FieldName = user => user.Admin
                            }
                        })
                        .ExecuteLazy().Value;

                    Assert.Empty(x["Admin"].Values);
                }
            }
        }
    }
}
