// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2435.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;

using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq.Indexing;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2435 : RavenTest
    {
        private class Index1 : AbstractIndexCreationTask<Person>
        {
            public Index1()
            {
                Map = people => from person in people
                                select new
                                       {
                                           Name = person.Name.StripHtml()
                                       };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class Transformer1 : AbstractTransformerCreationTask<Company>
        {
            public Transformer1()
            {
                TransformResults = results => from result in results
                                              select new
                                                     {
                                                         Name = result.Name.StripHtml()
                                                     };
            }
        }

        [Fact]
        public void StripHtmlShouldWorkForIndexesAndTransformers()
        {
            using (var store = NewDocumentStore())
            {
                new Index1().Execute(store);
                new Transformer1().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Name1"
                    });

                    session.Store(new Person
                    {
                        Name = "<b>Name2</b>"
                    });

                    session.Store(new Company
                    {
                        Name = "Name3"
                    });

                    session.Store(new Company
                    {
                        Name = "<b>Name4</b>"
                    });

                    session.Store(new Company
                    {
                        Name = ""
                    });

                    session.Store(new Company
                    {
                        Name = null
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store, timeout: TimeSpan.FromSeconds(60));

                var errors = store.DatabaseCommands.GetStatistics();

                using (var session = store.OpenSession())
                {
                    var people = session
                        .Query<Person>("Index1")
                        .ProjectFromIndexFieldsInto<Person>()
                        .ToList();

                    Assert.Equal(2, people.Count);
                    Assert.True(people.Any(x => x.Name == "Name1"));
                    Assert.True(people.Any(x => x.Name == "Name2"));
                }

                using (var session = store.OpenSession())
                {
                    var companies = session
                        .Query<Company>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .TransformWith<Company>("Transformer1")
                        .ToList();

                    Assert.Equal(4, companies.Count);
                    Assert.True(companies.Any(x => x.Name == "Name3"));
                    Assert.True(companies.Any(x => x.Name == "Name4"));
                    Assert.True(companies.Any(x => x.Name == string.Empty));
                    Assert.True(companies.Any(x => x.Name == null));
                }
            }
        }
    }
}
