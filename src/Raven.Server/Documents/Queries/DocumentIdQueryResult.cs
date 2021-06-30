using System.Collections.Generic;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Queries
{
    public class DocumentIdQueryResult : DocumentQueryResult
    {
        private readonly OperationCancelToken _token;

        public readonly Queue<string> DocumentIds = new Queue<string>();

        public DocumentIdQueryResult(OperationCancelToken token)
        {
            _token = token;
        }

        public override void AddResult(Document result)
        {
            using (result)
            {
                _token.Delay();
                DocumentIds.Enqueue(result.Id);
            }
        }
    }
}
