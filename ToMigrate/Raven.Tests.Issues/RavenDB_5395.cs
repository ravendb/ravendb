// -----------------------------------------------------------------------
//  <copyright file="RavenDB_5395.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_5395 : RavenTestBase
    {
        [Fact]
        public void SideBySideIndex_WillThrowIfMinimumEtagIsSpecifiedForMapReduce()
        {
            using (var store = NewDocumentStore())
            {
                new OldIndex().SideBySideExecute(store);
                new OldMapReduceIndex().SideBySideExecute(store);

                new OldIndex().SideBySideExecute(store, minimumEtagBeforeReplace: Etag.Parse(Guid.NewGuid().ToString()));

                var e = Assert.Throws<InvalidOperationException>(() =>
                {
                    new OldMapReduceIndex().SideBySideExecute(store, minimumEtagBeforeReplace: Etag.Parse(Guid.NewGuid().ToString()));
                });

                Assert.Equal(@"We do not support side-by-side execution for Map-Reduce indexes when 'minimum last indexed etag' scenario is used.", e.Message);
            }
        }

        private class Person
        {
            public string Id { get; set; }

            public string FirstName { get; set; }

            public string LastName { get; set; }
        }

        private class OldIndex : AbstractIndexCreationTask<Person>
        {
            public override string IndexName
            {
                get { return "The/Index"; }
            }

            public OldIndex()
            {
                Map = persons => from person in persons select new { person.FirstName };
            }
        }

        private class OldMapReduceIndex : AbstractIndexCreationTask<Person, OldMapReduceIndex.Result>
        {
            public class Result
            {
                public string FirstName { get; set; }

                public int Count { get; set; }
            }

            public OldMapReduceIndex()
            {
                Map = persons => from p in persons
                                 select new
                                 {
                                     p.FirstName,
                                     Count = 1
                                 };

                Reduce = results => from r in results
                                    group r by r.FirstName into g
                                    select new
                                    {
                                        FirstName = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }
    }
}