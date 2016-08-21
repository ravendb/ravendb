// -----------------------------------------------------------------------
//  <copyright file="DicWithDateTimeKeys.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class DicWithDateTimeKeys : RavenTestBase
    {
        private class A
        {
            public IDictionary<DateTimeOffset, string> Items { get; set; }
        }

        [Fact]
        public async Task CanSaveAndLoad()
        {
            using (var store = await GetDocumentStore())
            {
                var dateTimeOffset = DateTimeOffset.Now;
                using (var session = store.OpenSession())
                {
                    session.Store(new A
                    {
                        Items = new Dictionary<DateTimeOffset, string>
                        {
                            {dateTimeOffset, "a"}
                        }
                    });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var load = session.Load<A>(1);
                    Assert.Equal("a", load.Items[dateTimeOffset]);
                }
            }
        }
    }
}
