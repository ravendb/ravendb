using System;
using System.Collections.Generic;
using FastTests;
using Raven.Server.Config;
using Raven.Server.Utils.Enumerators;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13972_32_bits : RavenTestBase
    {
        [Fact]
        public void CanStreamDocumentsWithPulsatingReadTransaction()
        {
            using (var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Storage.ForceUsing32BitsPager)] = "true"

                }
            }))
            using (var store = GetDocumentStore(new Options
            {
                Server = server,
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Databases.PulseReadTransactionLimit)] = "0";
                }
            }))
            {
                int numberOfUsers = 2 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 10;

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < numberOfUsers; i++)
                    {
                        bulk.Store(new User(), "users/" + i);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var en = session.Advanced.Stream<User>((string)null);

                    var count = 0;

                    while (en.MoveNext())
                    {
                        count++;
                    }

                    Assert.Equal(numberOfUsers, count);
                }
            }
        }
    }
}
