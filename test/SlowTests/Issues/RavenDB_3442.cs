// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3442.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3442 : RavenTestBase
    {
        [Fact]
        public void WhereEqualsShouldSendSortHintsAndDynamicIndexesShouldSetAppropriateSortOptionsThen1()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Where(x => x.Count == 10)
                        .OrderBy(x => x.Count)
                        .ToList();
                }

                var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10));
                var index = indexes.Single(x => x.Name.StartsWith("Auto/"));

                Assert.Equal(1, index.Fields.Count);
            }
        }

    }
}
