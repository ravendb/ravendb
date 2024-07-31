using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Revisions
{
    public sealed class DeleteRevisionsOperation : IMaintenanceOperation<DeleteRevisionsOperation.Result>
    {
        private readonly Parameters _parameters;

        /// <summary>
        /// Initializes a new instance of the <see cref="DeleteRevisionsOperation"/> class.
        /// This operation allows deletion of revisions.
        /// </summary>
        /// <remarks>
        /// This is an <c>Admin</c> to operation.
        /// </remarks>

        /// <param name="documentIds">
        /// A list of document IDs whose revisions you want to delete. 
        /// Default is an empty list if not provided.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the <paramref name="documentIds"/> parameter is null or empty.
        /// </exception>

        /// <param name="removeForceCreatedRevisions">
        /// Indicates whether to include force-created revisions in the deletion. Default is <c>false</c>
        /// </param>

        public DeleteRevisionsOperation(List<string> documentIds, bool removeForceCreatedRevisions = false)
        {
            var parameters = new Parameters()
            {
                DocumentIds = documentIds,
                RemoveForceCreatedRevisions = removeForceCreatedRevisions
            };
            parameters.Validate();
            _parameters = parameters;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DeleteRevisionsOperation"/> class.
        /// This operation allows deletion of revisions.
        /// </summary>
        /// <remarks>
        /// This is an <c>Admin</c> to operation.
        /// </remarks>

        /// <param name="documentId">
        /// The ID of the document whose revisions you want to delete.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the <paramref name="documentId"/> parameter is null or empty.
        /// </exception>

        /// <param name="removeForceCreatedRevisions">
        /// Indicates whether to include force-created revisions in the deletion. Default is <c>false</c>
        /// </param>

        public DeleteRevisionsOperation(string documentId, bool removeForceCreatedRevisions = false)
        {
            var parameters = new Parameters()
            {
                DocumentIds = new List<string> { documentId },
                RemoveForceCreatedRevisions = removeForceCreatedRevisions
            };
            parameters.Validate();
            _parameters = parameters;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DeleteRevisionsOperation"/> class.
        /// This operation allows deletion of revisions.
        /// </summary>
        /// <remarks>
        /// This is an <c>Admin</c> to operation.
        /// </remarks>

        /// <param name="documentIds">
        /// A list of document IDs whose revisions you want to delete. 
        /// Default is an empty list if not provided.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the <paramref name="documentIds"/> parameter is null or empty.
        /// </exception>

        /// <param name="from">
        /// The start of the date range for the revisions to delete.
        /// </param>
        /// <param name="to">
        /// The end of the date range for the revisions to delete. 
        /// </param>
        /// <exception cref="ArgumentException">
        /// Throws an <see cref="ArgumentException"/> if <paramref name="to"/> is before <paramref name="from"/>.
        /// </exception>

        /// <param name="removeForceCreatedRevisions">
        /// Indicates whether to include force-created revisions in the deletion. Default is <c>false</c>
        /// </param>

        public DeleteRevisionsOperation(List<string> documentIds, DateTime? from, DateTime? to, bool removeForceCreatedRevisions = false)
        {
            var parameters = new Parameters()
            {
                DocumentIds = documentIds,
                From = from,
                To = to,
                RemoveForceCreatedRevisions = removeForceCreatedRevisions
            };
            parameters.Validate();
            _parameters = parameters;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DeleteRevisionsOperation"/> class.
        /// This operation allows deletion of revisions.
        /// </summary>
        /// <remarks>
        /// This is an <c>Admin</c> to operation.
        /// </remarks>

        /// <param name="documentId">
        /// The ID of the document whose revisions you want to delete.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the <paramref name="documentId"/> parameter is null or empty.
        /// </exception>

        /// <param name="from">
        /// The start of the date range for the revisions to delete.
        /// </param>
        /// <param name="to">
        /// The end of the date range for the revisions to delete. 
        /// </param>
        /// <exception cref="ArgumentException">
        /// Throws an <see cref="ArgumentException"/> if <paramref name="to"/> is before <paramref name="from"/>.
        /// </exception>

        /// <param name="removeForceCreatedRevisions">
        /// Indicates whether to include force-created revisions in the deletion. Default is <c>false</c>
        /// </param>

        public DeleteRevisionsOperation(string documentId, DateTime? from, DateTime? to, bool removeForceCreatedRevisions = false)
        {
            var parameters = new Parameters()
            {
                DocumentIds = new List<string> { documentId },
                From = from,
                To = to,
                RemoveForceCreatedRevisions = removeForceCreatedRevisions
            };
            parameters.Validate();
            _parameters = parameters;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DeleteRevisionsOperation"/> class.
        /// This operation allows deletion of revisions.
        /// </summary>
        /// <remarks>
        /// This is an <c>Admin</c> to operation.
        /// </remarks>

        /// <param name="documentId">
        /// The ID of the document whose revisions you want to delete.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the <paramref name="documentId"/> parameter is null or empty.
        /// </exception>

        /// <param name="revisionsChangeVectors">
        /// A list of change vectors corresponding to the revisions that the user wishes to delete.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the <paramref name="revisionsChangeVectors"/> parameter is null or empty.
        /// </exception>

        /// <param name="removeForceCreatedRevisions">
        /// Include also "Force Created" revisions on the deletion. Default is <c>false</c>
        /// </param>

        public DeleteRevisionsOperation(string documentId, List<string> revisionsChangeVectors, bool removeForceCreatedRevisions = false)
        {
            var parameters = new Parameters()
            {
                DocumentIds = new List<string> { documentId },
                RevisionsChangeVectors = revisionsChangeVectors,
                RemoveForceCreatedRevisions = removeForceCreatedRevisions
            };
            parameters.Validate();
            _parameters = parameters;
        }

        public RavenCommand<Result> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteRevisionsCommand(conventions, _parameters);
        }

        internal sealed class Parameters : ICloneable
        {
            public List<string> DocumentIds { get; set; }

            public bool RemoveForceCreatedRevisions { get; set; }

            // Either!
            public List<string> RevisionsChangeVectors { get; set; }

            // Or!
            public DateTime? From { get; set; } // start
            public DateTime? To { get; set; } // end

            internal void Validate()
            {
                if (DocumentIds.IsNullOrEmpty())
                    throw new ArgumentNullException(nameof(DocumentIds), $"request '{nameof(DocumentIds)}' cannot be null or empty.");

                foreach (var id in DocumentIds)
                {
                    if (string.IsNullOrEmpty(id))
                        throw new ArgumentNullException(nameof(DocumentIds), $"request '{nameof(DocumentIds)}' contains null or empty ids.");
                }

                if (RevisionsChangeVectors.IsNullOrEmpty())
                {
                    if (From.HasValue && To.HasValue && From >= To)
                        throw new ArgumentException($"{nameof(From)}, {nameof(To)}", "'After' must be greater then 'Before'.");
                }
                else
                {
                    if (DocumentIds.Count > 1)
                        throw new ArgumentException(nameof(DocumentIds), "The request must include exactly one document ID when deleting specific revisions by their change-vectors.");

                    if (From.HasValue)
                        throw new ArgumentException(nameof(From), $"request '{nameof(From)}' cannot have a value when deleting specific revisions by their change-vectors.");

                    if (To.HasValue)
                        throw new ArgumentException(nameof(To), $"request '{nameof(To)}' cannot have a value when deleting specific revisions by their change-vectors.");

                    foreach (var cv in RevisionsChangeVectors)
                    {
                        if (string.IsNullOrEmpty(cv))
                            throw new ArgumentNullException(nameof(RevisionsChangeVectors), $"request '{nameof(RevisionsChangeVectors)}' contains null or empty change-vectors.");
                    }
                }

            }

            public object Clone()
            {
                return new Parameters
                {
                    DocumentIds = this.DocumentIds,
                    RemoveForceCreatedRevisions = this.RemoveForceCreatedRevisions,
                    RevisionsChangeVectors = this.RevisionsChangeVectors,
                    From = this.From,
                    To = this.To,
                };
            }
        }

        public class Result
        {
            public long TotalDeletes { get; set; }
        }

    }
}
