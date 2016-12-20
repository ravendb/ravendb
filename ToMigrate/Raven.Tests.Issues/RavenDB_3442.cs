// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3442.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Abstractions.Indexing;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3442 : RavenTest
    {
        [Fact]
        public void WhereEqualsShouldSendSortHintsAndDynamicIndexesShouldSetAppropriateSortOptionsThen1()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Where(x => x.Age == 10)
                        .ToList();
                }

                var indexes = store.DatabaseCommands.GetIndexes(0, 10);
                var index = indexes.Single(x => x.Name.StartsWith("Auto/"));

                Assert.Equal(1, index.SortOptions.Count);
                Assert.Equal(SortOptions.Int, index.SortOptions["Age"]);
            }
        }

    }
}
