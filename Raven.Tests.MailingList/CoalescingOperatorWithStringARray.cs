// -----------------------------------------------------------------------
//  <copyright file="CoalescingOperatorWithStringARray.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using System.Threading;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class CoalescingOperatorWithStringArray : RavenTest
    {

        [Fact]
        public void CanQueryIndexContainingStringArray()
        {
            using (var store = NewDocumentStore())
            {
                new MyIndex().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new A { Id = "1", Bs = new[] { new B { Id = "1", Values = new[] { "A1", "A2" } }, new B { Id = "2", Values = new[] { "B1", "B2" } } } });
                    session.Store(new A { Id = "2", Bs = new[] { new B { Id = "1", Values = new[] { "A1", "A2" } }, new B { Id = "2", Values = new[] { "B1", "B2" } } } });
                    session.Store(new A { Id = "3", Bs = new[] { new B { Id = "1", Values = new[] { "A1", "A2" } }, new B { Id = "2", Values = new[] { "B1", "B2" } } } });
                    session.Store(new A { Id = "4", Bs = new[] { new B { Id = "1", Values = new[] { "A1", "xxx" } }, new B { Id = "2", Values = new[] { "B1", "B2" } } } });
                    session.Store(new A { Id = "5", Bs = new[] { new B { Id = "1", Values = new[] { "A1", "A2" } }, new B { Id = "2", Values = new[] { "B1", "yyy" } } } });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var ravenErrors = store.DatabaseCommands.GetStatistics().Errors;
                Assert.True(ravenErrors.Length == 0);
                Assert.True(store.DatabaseCommands.GetStatistics().Indexes.All(x => x.IndexingErrors == 0));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyIndex.ReduceResult, MyIndex>().Count(x => x.Values.Any(y => y == "xxx"));
                    Assert.Equal(1, count);
                }
            }
        }

        public class A
        {
            public string Id { get; set; }
            public B[] Bs { get; set; }
        }

        public class B
        {
            public string Id { get; set; }
            public string[] Values { get; set; }
        }



        public class MyIndex : AbstractIndexCreationTask<A, MyIndex.ReduceResult>
        {
            public class ReduceResult
            {
                public string AId { get; set; }
                public string BId { get; set; }
                public string[] Values { get; set; }
                public int Count { get; set; }
            }

            public MyIndex()
            {
                Map = values => from a in values
                                from b in a.Bs
                                select new
                                {
                                    AId = a.Id,
                                    BId = b.Id,
                                    Values = b.Values ?? new string[0],
                                    Count = 1
                                };
                Reduce = results => from r in results
                                    group r by new { r.AId, r.BId }
                                        into rGroup
                                        select new
                                        {
                                            rGroup.Key.AId,
                                            rGroup.Key.BId,
                                            rGroup.First().Values,
                                            Count = rGroup.Sum(x => x.Count)
                                        };
            }
        }
    }
}