using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.Server.PeriodicExport;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public class GetPeriodicExportStatusOperation : IServerOperation<GetPeriodicExportStatusOperationResult>
    {
        private string _databaseName;

        public GetPeriodicExportStatusOperation(string DatabaseName)
        {
            _databaseName = DatabaseName;
        }
        public RavenCommand<GetPeriodicExportStatusOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetPeriodicExportStatusCommand(_databaseName);
        }
    }

    public class GetPeriodicExportStatusCommand : RavenCommand<GetPeriodicExportStatusOperationResult>
    {
        private string _databaseName;

        public GetPeriodicExportStatusCommand(string databaseName)
        {
            _databaseName = databaseName;
        }

        public override bool IsReadRequest => true;
        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/periodic-export-stats";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if(response == null)
                ThrowInvalidResponse();
            Result = JsonDeserializationClient.GetExpirationBundleStatusOperationResult(response);
        }
    }

    public class GetPeriodicExportStatusOperationResult
    {
        public PeriodicExportStatus Status;
    }
}
