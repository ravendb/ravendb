using System.Collections.Generic;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Revisions
{
    public class RevertDocumentsToRevisionsOperation : IOperation
    {
        private readonly Dictionary<string, string> _idToChangeVector;

        public RevertDocumentsToRevisionsOperation(Dictionary<string, string> idToChangeVector)
        {
            _idToChangeVector = idToChangeVector;
        }

        public RevertDocumentsToRevisionsOperation(string id, string cv)
        {
            _idToChangeVector = new Dictionary<string, string>() { { id, cv } };
        }

        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache) => new RevertDocumentsToRevisionsCommand(_idToChangeVector);
    }

}
