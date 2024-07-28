using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Revisions
{
    public class DeleteRevisionsManuallyOperation : IOperation<DeleteRevisionsManuallyOperation.Result>
    {
        private DeleteRevisionsRequest _request;

        /// <summary>
        /// Initializes a new instance of the <see cref="DeleteRevisionsManuallyOperation"/> class.
        /// This operation allows manual deletion of revisions, provided that the configuration 
        /// for those revisions, <c>AllowDeleteRevisionsManually</c>, is set to <c>true</c>.
        /// </summary>
        /// <param name="revisionsChangeVectors">
        /// A list of change vectors corresponding to the revisions that the user wishes to delete.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the <paramref name="revisionsChangeVectors"/> parameter is null or empty.
        /// </exception>
        /// <remarks>
        /// This operation will only proceed if the server's configuration allows manual deletion 
        /// of revisions by setting <c>AllowDeleteRevisionsManually</c> to <c>true</c>.
        /// </remarks>

        public DeleteRevisionsManuallyOperation(string documentId, List<string> revisionsChangeVecotors)
        {
            if (revisionsChangeVecotors == null)
            {
                throw new ArgumentNullException(nameof(revisionsChangeVecotors), "'revisionsChangeVecotors' cannot be both null or empty.");
            }

            _request = new DeleteRevisionsRequest
            {
                DocumentId = documentId,
                RevisionsChangeVectors = revisionsChangeVecotors
            };

            _request.Validate();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DeleteRevisionsManuallyOperation"/> class.
        /// This operation allows manual deletion of document revisions, provided that the configuration 
        /// for those revisions, <c>AllowDeleteRevisionsManually</c>, is set to <c>true</c>.
        /// </summary>
        /// <param name="documentId">
        /// The ID of the document whose revisions are to be deleted.
        /// </param>
        /// <param name="maxDeletes">
        /// The maximum number of revisions to delete. Defaults to 1024.
        /// </param>
        /// <param name="after">
        /// Optional parameter to specify the starting date-time after which revisions should be deleted.
        /// </param>
        /// <param name="before">
        /// Optional parameter to specify the ending date-time before which revisions should be deleted.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the <paramref name="documentId"/> parameter is null or empty.
        /// </exception>
        /// <remarks>
        /// This operation will only proceed if the server's configuration allows manual deletion 
        /// of revisions by setting <c>AllowDeleteRevisionsManually</c> to <c>true</c>.
        /// </remarks>

        public DeleteRevisionsManuallyOperation(string documentId, long maxDeletes = 1024, DateTime? after = null, DateTime? before = null)
        {
            if (string.IsNullOrEmpty(documentId))
            {
                throw new ArgumentNullException(nameof(documentId), "'revisionsChangeVecotors' cannot be both null or empty.");
            }

            _request = new DeleteRevisionsRequest
            {
                DocumentId = documentId,
                MaxDeletes = maxDeletes,
                After = after,
                Before = before
            };

            _request.Validate();
        }

        public RavenCommand<Result> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new DeleteRevisionsManuallyCommand(_request);
        }

        public class Result
        {
            public long TotalDeletes { get; set; }
        }
    }
}
