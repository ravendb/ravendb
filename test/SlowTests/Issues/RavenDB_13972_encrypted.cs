using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Server.Config;
using Raven.Server.Utils.Enumerators;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13972_encrypted : RavenDB_13972
    {
        public RavenDB_13972_encrypted(ITestOutputHelper output) : base(output)
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

            EncryptedServer(out var certificates, out string dbName);

            using (var storeToExport = GetDocumentStore(new Options
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseName = s => dbName,
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Databases.PulseReadTransactionLimit)] = "0";
                    record.Encrypted = true;
                }
            }))
            using (var server = GetNewServer(new ServerCreationOptions()))
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
            EncryptedServer(out var certificates, out string dbName);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseName = s => dbName,
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Databases.PulseReadTransactionLimit)] = "0";
                    record.Encrypted = true;
                }
            }))
            {
                CanStreamDocumentsWithPulsatingReadTransaction_ActualTest(numberOfUsers, numberOfOrders, deleteUserFactor, store);
            }
        }

        [Theory]
        [InlineData(2 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 10)]
        [InlineData(4 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 3)]
        [InlineData(10 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 3)]
        public void CanStreamQueryWithPulsatingReadTransaction(int numberOfUsers)
        {
            EncryptedServer(out var certificates, out string dbName);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseName = s => dbName,
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Databases.PulseReadTransactionLimit)] = "0";
                    record.Encrypted = true;
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
            EncryptedServer(out var certificates, out string dbName);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseName = s => dbName,
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Databases.PulseReadTransactionLimit)] = "0";
                    record.Encrypted = true;
                }
            }))
            {
                CanStreamCollectionQueryWithPulsatingReadTransaction_ActualTest(numberOfUsers, store);
            }
        }
    }
}
