// -----------------------------------------------------------------------
//  <copyright file="CachingOfLongQueries.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Querying
{
    public class CachingOfLongQueries : RavenTest
    {
        [Fact]
        public void ShouldWork()
        {
            using (var store = NewRemoteDocumentStore(fiddler:true))
            {
                var val = new string('a', 2048);
                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Val = val });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Query<Item>()
                        .Where(x => x.Val == val)
                        .ToList());

                    val = new string('b', 2048);

                    Assert.Empty(session.Query<Item>()
                        .Where(x => x.Val == val)
                        .ToList());


                }
            }
        }

        public class Item
        {
            public string Val;
        }
    }
}
