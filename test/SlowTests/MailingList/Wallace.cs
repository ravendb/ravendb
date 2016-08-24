// -----------------------------------------------------------------------
//  <copyright file="Wallace.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class Wallace : RavenTestBase
    {
        [Fact]
        public void CanGetProperErrorFromComputedOrderBy()
        {
            using(var store = GetDocumentStore())
            {
                using(var session = store.OpenSession())
                {
                    var argumentException = Assert.Throws<ArgumentException>(() => session.Query<Order>().OrderBy(x => x.OrderLines.Last().Quantity).ToList());

                    Assert.Equal("Could not understand expression: .OrderBy(x => x.OrderLines.Last().Quantity)", argumentException.Message);
                    Assert.Equal("Not supported computation: x.OrderLines.Last().Quantity. You cannot use computation in RavenDB queries (only simple member expressions are allowed).",
                        argumentException.InnerException.Message);
                }
            }
        }
    }
}
