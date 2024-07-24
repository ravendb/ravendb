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

        public DeleteRevisionsManuallyOperation(List<string> revisionsChangeVecotors)
        {
            if (revisionsChangeVecotors == null)
            {
                throw new ArgumentNullException(nameof(revisionsChangeVecotors), "'revisionsChangeVecotors' cannot be both null or empty.");
            }

            _request = new DeleteRevisionsRequest { RevisionsChangeVecotors = revisionsChangeVecotors };
        }

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

            _request.ValidateDocumentId();
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
