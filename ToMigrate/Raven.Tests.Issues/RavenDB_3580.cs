using Raven.Abstractions.Data;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3580 : RavenTest
    {
        [Fact]
        public void CanProperlyComputeTotalsWithoutAbandonedOrDisabled()
        {
            using (var store = NewDocumentStore()) 
            {
                var companiesIndex = new RavenDB_2607.CompaniesIndex();
                var usersIndex = new RavenDB_2607.UsersIndex();
                var usersAndCompaniesIndex = new RavenDB_2607.UsersAndCompaniesIndex();

                store.ExecuteIndex(companiesIndex);
                store.ExecuteIndex(usersIndex);
                store.ExecuteIndex(usersAndCompaniesIndex);

                store.DatabaseCommands.SetIndexPriority(usersAndCompaniesIndex.IndexName, IndexingPriority.Abandoned);

                WaitForIndexing(store);

                store.DatabaseCommands.Admin.StopIndexing();

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "A"
                    });

                    session.SaveChanges();
                }

                var databaseStatistics = store.DatabaseCommands.GetStatistics();
                Assert.Equal(4, databaseStatistics.StaleIndexes.Length);
                Assert.Equal(4, databaseStatistics.Indexes.Length);
                Assert.Equal(3, databaseStatistics.CountOfStaleIndexesExcludingDisabledAndAbandoned);
                Assert.Equal(3, databaseStatistics.CountOfIndexesExcludingDisabledAndAbandoned);

            }
        }

    }
}
