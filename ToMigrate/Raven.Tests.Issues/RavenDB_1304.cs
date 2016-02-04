// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1304.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_1304 : RavenTest
    {
        [Theory]
        [PropertyData("Storages")]
        public void StoringObjectsWithIdsThatDifferAfterLength127ShouldNotThrowConcurrencyException(string storage)
        {
            using (var store = NewDocumentStore(requestedStorage: storage))
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;

                    session.Store(new { }, new string('a', 127) + "1");
                    session.Store(new { }, new string('a', 127) + "2");

                    Assert.DoesNotThrow(() => session.SaveChanges());
                }
            }
        }
    }
}
