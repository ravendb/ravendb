using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Indexes;
using Sparrow.Json;

namespace Tests.Infrastructure.Operations
{
    public class GetIndexStalenessOperation : IMaintenanceOperation<GetIndexStalenessCommand.IndexStaleness>
    {
        private readonly string _indexName;

        public GetIndexStalenessOperation(string indexName)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        }

        public RavenCommand<GetIndexStalenessCommand.IndexStaleness> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexStalenessCommand(_indexName, nodeTag: null);
        }
    }
}
