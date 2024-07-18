using System;
using System.Linq;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class EditDocumentsCompressionCommand : UpdateDatabaseCommand
    {
        public DocumentsCompressionConfiguration Configuration;
        
        public EditDocumentsCompressionCommand()
        {
        }
        
        public void UpdateDatabaseRecord(DatabaseRecord databaseRecord)
        {
            databaseRecord.DocumentsCompression = Configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.DocumentsCompression = Configuration;
        }
        
        public EditDocumentsCompressionCommand(DocumentsCompressionConfiguration configuration, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            if (configuration?.Collections.Length > 0)
            {
                configuration.Collections = configuration.Collections.ToHashSet(StringComparer.OrdinalIgnoreCase).ToArray();
            }

            Configuration = configuration;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }

        public override void AssertLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54200, serverStore) == false)
                return;

            var licenseStatus = serverStore.LicenseManager.LoadAndGetLicenseStatus(serverStore);
            if (licenseStatus.HasDocumentsCompression)
                return;

            if (databaseRecord.DocumentsCompression == null)
                return;

            if (databaseRecord.DocumentsCompression.CompressAllCollections == false && databaseRecord.DocumentsCompression.CompressRevisions == false)
                return;

            throw new LicenseLimitException(LimitType.DocumentsCompression, "Your license doesn't support adding Documents Compression feature.");

        }
    }
}
