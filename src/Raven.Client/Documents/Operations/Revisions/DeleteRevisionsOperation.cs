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
    public class DeleteRevisionsOperation : IOperation
    {
        private DeleteRevisionsRequest _request;

        public DeleteRevisionsOperation(DeleteRevisionsRequest request)
        {
            if (request == null)
            {
                throw new ArgumentNullException(nameof(request), "request cannot be null.");
            }

            if (request.MaxDeletes <= 0)
            {
                throw new ArgumentNullException(nameof(request), "request 'MaxDeletes' have to be greater then 0.");
            }

            if (request.DocumentIds.IsNullOrEmpty() && request.RevisionsChangeVecotors.IsNullOrEmpty())
            {
                throw new ArgumentNullException(nameof(request), "request 'DocumentIds' and 'RevisionsChangeVecotors' cannot be both null or empty.");
            }

            _request = request;
        }

        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            throw new NotImplementedException();
        }
    }
}
