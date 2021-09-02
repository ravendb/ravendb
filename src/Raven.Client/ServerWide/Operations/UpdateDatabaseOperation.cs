using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class UpdateDatabaseOperation : IServerOperation<DatabasePutResult>
    {
        private readonly DatabaseRecord _databaseRecord;
        private readonly long _etag;
        private readonly int _replicationFactor;

        public UpdateDatabaseOperation(DatabaseRecord databaseRecord, long etag)
        {
            if (databaseRecord == null)
                throw new ArgumentNullException(nameof(databaseRecord));

            ResourceNameValidator.AssertValidDatabaseName(databaseRecord.DatabaseName);
            _databaseRecord = databaseRecord;
            _etag = etag;
            _replicationFactor = databaseRecord.Topology?.ReplicationFactor > 0 ? databaseRecord.Topology.ReplicationFactor : throw new ArgumentException($"{nameof(databaseRecord)}.{nameof(databaseRecord.Topology)}.{nameof(databaseRecord.Topology.ReplicationFactor)} is missing.");
        }

        public UpdateDatabaseOperation(DatabaseRecord databaseRecord, int replicationFactor, long etag)
        {
            if (databaseRecord == null)
                throw new ArgumentNullException(nameof(databaseRecord));

            if (replicationFactor <= 0)
                throw new ArgumentException(nameof(replicationFactor));

            ResourceNameValidator.AssertValidDatabaseName(databaseRecord.DatabaseName);
            _databaseRecord = databaseRecord;
            _etag = etag;
            _replicationFactor = replicationFactor;
        }

        public RavenCommand<DatabasePutResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new CreateDatabaseOperation.CreateDatabaseCommand(_databaseRecord, _replicationFactor, _etag);
        }
    }
}
