using System.Collections.Generic;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Queries
{
    public class DocumentIdQueryResult : DocumentQueryResult
    {
        private readonly Queue<string> _resultIds;
        private readonly OperationCancelToken _token;

        public DocumentIdQueryResult(Queue<string> resultIds, OperationCancelToken token)
        {
            _resultIds = resultIds;
            _token = token;
        }

        public override void AddResult(Document result)
        {
            using (result)
            {
                _token.Delay();
                _resultIds.Enqueue(result.Id);
            }
        }
    }
}
