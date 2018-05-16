using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class UpdateDatabaseOperation : IServerOperation<DatabasePutResult>
    {
        private readonly DatabaseRecord _databaseRecord;
        private readonly long _etag;

        public UpdateDatabaseOperation(DatabaseRecord databaseRecord, long etag)
        {
            Helpers.AssertValidDatabaseName(databaseRecord.DatabaseName);
            _databaseRecord = databaseRecord;
            _etag = etag;
        }

        public RavenCommand<DatabasePutResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new CreateDatabaseOperation.CreateDatabaseCommand(_databaseRecord, etag: _etag);
        }
    }
}
