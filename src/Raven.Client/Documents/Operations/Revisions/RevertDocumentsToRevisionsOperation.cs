using System.Collections.Generic;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Revisions
{
    internal class RevertDocumentsToRevisionsOperation : IMaintenanceOperation
    {
        private readonly Dictionary<string, string> _idToChangeVector;

        public RevertDocumentsToRevisionsOperation(Dictionary<string, string> idToChangeVector)
        {
            _idToChangeVector = idToChangeVector;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context) => new RevertDocumentsToRevisionsCommand(_idToChangeVector);
    }

}
