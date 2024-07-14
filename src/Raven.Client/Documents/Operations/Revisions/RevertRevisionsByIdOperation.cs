using System;
using System.Collections.Generic;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Revisions
{
    public class RevertRevisionsByIdOperation : IOperation
    {
        private readonly Dictionary<string, string> _idToChangeVector;

        /// <summary>
        /// Client Operation to revert documents to specified revisions based on provided change vectors.
        /// </summary>
        /// <param name="idToChangeVector">A dictionary where each key is a document ID and each value is the revision change vector for that document.
        /// each pair contains the ID of the document to revert (as a key), and the change vector of the revision to which the document should be reverted (as a value).
        /// </param>
        public RevertRevisionsByIdOperation(Dictionary<string, string> idToChangeVector)
        {
            if (idToChangeVector == null)
            {
                throw new ArgumentNullException(nameof(idToChangeVector), "idToChangeVector cannot be null.");
            }

            if (idToChangeVector.Count == 0)
            {
                throw new ArgumentException("idToChangeVector must contain at least one item.", nameof(idToChangeVector));
            }

            _idToChangeVector = idToChangeVector;
        }

        /// <summary>
        /// Client Operation to revert documents to specified revisions based on provided change vectors.
        /// </summary>
        /// <param name="id">The ID of the document to revert.</param>
        /// <param name="cv">The change vector of the revision to which the document should be reverted.</param>
        public RevertRevisionsByIdOperation(string id, string cv)
        {
            if (string.IsNullOrEmpty(id))
            {
                throw new ArgumentException("id cannot be null or empty.", nameof(id));
            }

            if (string.IsNullOrEmpty(cv))
            {
                throw new ArgumentException("cv cannot be null or empty.", nameof(cv));
            }

            _idToChangeVector = new Dictionary<string, string>() { { id, cv } };
        }

        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache) => new RevertRevisionsByIdCommand(_idToChangeVector);
    }

}
