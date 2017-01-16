using System;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Indexing;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.Indexes
{
    public class PutIndexOperation : IAdminOperation<PutIndexResult>
    {
        private readonly string _indexName;
        private readonly IndexDefinition _indexDefinition;

        public PutIndexOperation(string indexName, IndexDefinition indexDefinition)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));
            if (indexDefinition == null)
                throw new ArgumentNullException(nameof(indexDefinition));

            _indexName = indexName;
            _indexDefinition = indexDefinition;
        }

        public RavenCommand<PutIndexResult> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new PutIndexCommand(conventions, context, _indexName, _indexDefinition);
        }
    }
}