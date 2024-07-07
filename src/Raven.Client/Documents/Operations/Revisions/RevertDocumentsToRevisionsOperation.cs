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

        /// <summary>
        /// Client Operation to revert documents to specified revisions based on provided change vectors.
        /// </summary>
        /// <param name="idToChangeVector">A dictionary where each key is a document ID and each value is the revision change vector for that document.
        /// each pair contains the ID of the document to revert (as a key), and the change vector of the revision to which the document should be reverted (as a value).
        /// </param>
        public RevertDocumentsToRevisionsOperation(Dictionary<string, string> idToChangeVector)
        {
            _idToChangeVector = idToChangeVector;
        }

        /// <summary>
        /// Client Operation to revert documents to specified revisions based on provided change vectors.
        /// </summary>
        /// <param name="id">The ID of the document to revert.</param>
        /// <param name="cv">The change vector of the revision to which the document should be reverted.</param>
        public RevertDocumentsToRevisionsOperation(string id, string cv)
        {
            _idToChangeVector = new Dictionary<string, string>() { { id, cv } };
        }

        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache) => new RevertDocumentsToRevisionsCommand(_idToChangeVector);
    }

}
