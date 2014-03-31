// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1304.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1304 : RavenTest
    {
        [Fact]
        public void StoringObjectsWithIdsThatDifferAfterLength127ShouldNotThrowConcurrencyException()
        {
            using (var store = NewDocumentStore(requestedStorage:"esent"))
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;

                    session.Store(new { }, new string('a', 127) + "1");
                    session.Store(new { }, new string('a', 127) + "2");

                    Assert.DoesNotThrow(session.SaveChanges);
                }
            }
        }
    }
}