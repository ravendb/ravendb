// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3344.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3344 : RavenTestBase
    {
        public RavenDB_3344(ITestOutputHelper output) : base(output)
        {
        }

        private class Index1 : AbstractIndexCreationTask<Person>
        {
            public class Result
            {
                public string CurrentName { get; set; }

                public string PreviousName { get; set; }
            }

            public Index1()
            {
                Map = persons => from person in persons
                                 let metadata = MetadataFor(person)
                                 from name in metadata.Value<string>("Names").Split(',', StringSplitOptions.None)
                                 select new
                                 {
                                     CurrentName = person.Name,
                                     PreviousName = person.Name
                                 };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        [Fact]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new Index1().Execute(store);

                using (var session = store.OpenSession())
                {
                    var person = new Person { Name = "John" };
                    session.Store(person);
                    var metadata = session.Advanced.GetMetadataFor(person);
                    metadata["Names"] = "James,Jonathan";

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Query<Person, Index1>()
                        .ProjectInto<Index1.Result>()
                        .ToList();

                    Assert.Equal(2, results.Count);
                }
            }
        }
    }
}
