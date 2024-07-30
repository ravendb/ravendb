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

        public DeleteRevisionsOperation(List<string> documentIds, bool includeForceCreated = false)
        {
            var parameters = new Parameters()
            {
                DocumentIds = documentIds,
                IncludeForceCreated = includeForceCreated
            };
            parameters.Validate();
            _parameters = parameters;
        }

        public DeleteRevisionsOperation(string documentId, bool includeForceCreated = false)
        {
            var parameters = new Parameters()
            {
                DocumentIds = new List<string> { documentId },
                IncludeForceCreated = includeForceCreated
            };
            parameters.Validate();
            _parameters = parameters;
        }

        public DeleteRevisionsOperation(List<string> documentIds, DateTime? from, DateTime? to, bool includeForceCreated = false)
        {
            var parameters = new Parameters()
            {
                DocumentIds = documentIds,
                After = from,
                Before = to,
                IncludeForceCreated = includeForceCreated
            };
            parameters.Validate();
            _parameters = parameters;
        }

        public DeleteRevisionsOperation(string documentId, DateTime? from, DateTime? to, bool includeForceCreated = false)
        {
            var parameters = new Parameters()
            {
                DocumentIds = new List<string> { documentId },
                After = from,
                Before = to,
                IncludeForceCreated = includeForceCreated
            };
            parameters.Validate();
            _parameters = parameters;
        }

        public DeleteRevisionsOperation(string documentId, List<string> revisionsChangeVectors, bool includeForceCreated = false)
        {
            var parameters = new Parameters()
            {
                DocumentIds = new List<string> { documentId },
                RevisionsChangeVectors = revisionsChangeVectors,
                IncludeForceCreated = includeForceCreated
            };
            parameters.Validate();
            _parameters = parameters;
        }

        internal DeleteRevisionsOperation(Parameters parameters)
        {
            parameters.Validate();
            _parameters = parameters;
        }

        public RavenCommand<Result> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteRevisionsCommand(conventions, _parameters);
        }

        public sealed class Parameters : ICloneable
        {
            public List<string> DocumentIds { get; set; }

            public bool IncludeForceCreated { get; set; }

            // Either!
            public List<string> RevisionsChangeVectors { get; set; }

            // Or!
            public DateTime? After { get; set; } // start
            public DateTime? Before { get; set; } // end

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
                    if (After.HasValue && Before.HasValue && After >= Before)
                        throw new ArgumentException($"{nameof(After)}, {nameof(Before)}", "'After' must be greater then 'Before'.");
                }
                else
                {
                    if (DocumentIds.Count > 1)
                        throw new ArgumentException(nameof(DocumentIds), "The request must include exactly one document ID when deleting specific revisions by their change-vectors.");

                    if (After.HasValue)
                        throw new ArgumentException(nameof(After), $"request '{nameof(After)}' cannot have a value when deleting specific revisions by their change-vectors.");

                    if (Before.HasValue)
                        throw new ArgumentException(nameof(Before), $"request '{nameof(Before)}' cannot have a value when deleting specific revisions by their change-vectors.");

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
                    IncludeForceCreated = this.IncludeForceCreated,
                    RevisionsChangeVectors = this.RevisionsChangeVectors,
                    After = this.After,
                    Before = this.Before,
                };
            }
        }

        public class Result
        {
            public long TotalDeletes { get; set; }
        }

    }
}
