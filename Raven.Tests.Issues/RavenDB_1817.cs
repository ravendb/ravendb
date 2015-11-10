// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1817.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1817 : RavenTest
    {
        private class MapIndex : AbstractIndexCreationTask<Person>
        {
            public MapIndex()
            {
                Map = people => from person in people
                                select new
                                       {
                                           Name = person.Name
                                       };
            }
        }

        private class MapReduceIndex : AbstractIndexCreationTask<Person, MapReduceIndex.Result>
        {
            public class Result
            {
                public string Name { get; set; }

                public int Count { get; set; }
            }

            public MapReduceIndex()
            {
                Map = people => from person in people
                                select new
                                {
                                    Name = person.Name,
                                    Count = 1
                                };

                Reduce = results => from result in results
                                    group result by result.Name into g
                                    select new
                                           {
                                               Name = g.Key,
                                               Count = g.Sum(x => x.Count)
                                           };
            }
        }

        [Fact]
        public void DeleteByIndexShouldThrowIfIndexDoesNotExist()
        {
            using (var store = NewDocumentStore())
            {
                var e = Assert.Throws<InvalidOperationException>(() =>
                {
                    store
                        .DatabaseCommands
                        .DeleteByIndex("SomeIndex", new IndexQuery());
                });

                Assert.Equal("There is no index named: SomeIndex", e.Message);
            }
        }

        [Fact]
        public void DeleteByIndexShouldWorkForMapIndexes()
        {
            using (var store = NewDocumentStore())
            {
                var index = new MapIndex();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Name = "Name1" });
                    session.Store(new Person { Name = "Name1" });
                    session.Store(new Person { Name = "Name2" });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                store
                    .DatabaseCommands
                    .DeleteByIndex(index.IndexName, new IndexQuery { Query = "Name:Name1" })
                    .WaitForCompletion();

                using (var session = store.OpenSession())
                {
                    var people = session
                        .Query<Person>(index.IndexName)
                        .ToList();

                    Assert.Equal(1, people.Count);
                    Assert.Equal("Name2", people[0].Name);
                }
            }
        }

        [Fact]
        public void DeleteByIndexShouldThrowForMapReduceIndexes()
        {
            using (var store = NewDocumentStore())
            {
                var index = new MapReduceIndex();
                index.Execute(store);

                var e = Assert.Throws<ErrorResponseException>(() => store.DatabaseCommands.DeleteByIndex(index.IndexName, new IndexQuery()));

                Assert.Contains("Cannot execute DeleteByIndex operation on Map-Reduce indexes.", e.Message);
            }
        }
    }
}
