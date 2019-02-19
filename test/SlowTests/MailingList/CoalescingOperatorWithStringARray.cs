// -----------------------------------------------------------------------
//  <copyright file="CoalescingOperatorWithStringARray.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class CoalescingOperatorWithStringArray : RavenTestBase
    {
        [Fact]
        public void CanQueryIndexContainingStringArray()
        {
            using (var store = GetDocumentStore())
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

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyIndex.ReduceResult, MyIndex>().Count(x => x.Values.Any(y => y == "xxx"));
                    Assert.Equal(1, count);
                }
            }
        }

        private class A
        {
            public string Id { get; set; }
            public B[] Bs { get; set; }
        }

        private class B
        {
            public string Id { get; set; }
            public string[] Values { get; set; }
        }

        private class MyIndex : AbstractIndexCreationTask<A, MyIndex.ReduceResult>
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
