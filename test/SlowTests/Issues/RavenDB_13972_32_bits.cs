using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Config;
using Raven.Server.Utils.Enumerators;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13972_32_bits : RavenDB_13972
    {
        public RavenDB_13972_32_bits(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(2 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 10, 2, 2, 2 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 10, 2)]
        [InlineData(2 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 10, 2, 2, 0, 2)]
        [InlineData(2 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 10, 2, 2, 2 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 10, 3)]
        [InlineData(4 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 3, 2, 2, 0, 3)]
        [InlineData(4 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 3, 2, 2, 4 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 3, 2)]
        public async Task CanExportWithPulsatingReadTransaction(int numberOfUsers, int numberOfCountersPerUser, int numberOfRevisionsPerDocument, int numberOfOrders, int deleteUserFactor)
        {
            var file = GetTempFileName();
            var fileAfterDeletions = GetTempFileName();

            using (var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Storage.ForceUsing32BitsPager)] = "true"

                }
            }))
            using (var storeToExport = GetDocumentStore(new Options
            {
                Server = server,
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Databases.PulseReadTransactionLimit)] = "0";
                }
            }))
            using (var storeToImport = GetDocumentStore(new Options
            {
                Server = server
            }))
            using (var storeToAfterDeletions = GetDocumentStore(new Options
            {
                Server = server
            }))
            {
                await CanExportWithPulsatingReadTransaction_ActualTest(numberOfUsers, numberOfCountersPerUser, numberOfRevisionsPerDocument, numberOfOrders, deleteUserFactor, storeToExport, file, storeToImport, fileAfterDeletions, storeToAfterDeletions);
            }
        }

        [Theory]
        [InlineData(2 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 10, 0, 2)]
        [InlineData(2 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 10, 2 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 10, 3)]
        [InlineData(4 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 3, 0, 3)]
        [InlineData(4 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 3, 4 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 3, 2)]
        public void CanStreamDocumentsWithPulsatingReadTransaction(int numberOfUsers, int numberOfOrders, int deleteUserFactor)
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
                CanStreamDocumentsWithPulsatingReadTransaction_ActualTest(numberOfUsers, numberOfOrders, deleteUserFactor, store);
            }
        }

        [Theory]
        [InlineData(2 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 10)]
        [InlineData(4 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 3)]
        public void CanStreamQueryWithPulsatingReadTransaction(int numberOfUsers)
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
                CanStreamQueryWithPulsatingReadTransaction_ActualTest(numberOfUsers, store);
            }
        }

        [Theory]
        [InlineData(2 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 10)]
        [InlineData(4 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 3)]
        public void CanStreamCollectionQueryWithPulsatingReadTransaction(int numberOfUsers)
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
                CanStreamCollectionQueryWithPulsatingReadTransaction_ActualTest(numberOfUsers, store);
            }
        }
    }
}
