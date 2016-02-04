// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2615.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Raven.Tests.Common.Dto.TagCloud;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2615 : RavenTest
    {
        [Fact]
        public void ShouldRecoverLastCollectionEtags()
        {
            var dataDir = NewDataPath();
            var ravenDbServer = GetNewServer(runInMemory: false, dataDirectory:dataDir);

            var companiesIndex = new RavenDB_2607.CompaniesIndex();
            var usersIndex = new RavenDB_2607.UsersIndex();

            using (var store = ravenDbServer.DocumentStore)
            {
                store.ExecuteIndex(companiesIndex);
                store.ExecuteIndex(usersIndex);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Id = "companies/1", Name = "test"});
                    session.Store(new User { Id = "users/1", Name = "test" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);
            }
            
            // restart server
            ravenDbServer.Dispose();
            ravenDbServer = GetNewServer(runInMemory: false, dataDirectory: dataDir);

            var companiesCollectioEtag = ravenDbServer.SystemDatabase.LastCollectionEtags.GetLastEtagForCollection("Companies");
            var usersCollectionEtag = ravenDbServer.SystemDatabase.LastCollectionEtags.GetLastEtagForCollection("Users");

            Assert.NotNull(companiesCollectioEtag);
            Assert.NotNull(usersCollectionEtag);

            using (var store = ravenDbServer.DocumentStore)
            {
                store.DatabaseCommands.Admin.StopIndexing();

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Id = "companies/2", Name = "test"
                    });

                    session.SaveChanges();
                }

                var staleIndexes = store.DatabaseCommands.GetStatistics().StaleIndexes;

                Assert.DoesNotContain(usersIndex.IndexName, staleIndexes);
            }
        }
    }
}
