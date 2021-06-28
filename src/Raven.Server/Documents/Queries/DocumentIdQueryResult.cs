using System.Collections.Generic;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Queries
{
    public class DocumentIdQueryResult : DocumentQueryResult
    {
        private readonly OperationCancelToken _token;

        public Queue<string> ResultIds { get; } = new Queue<string>();

        public DocumentIdQueryResult(OperationCancelToken token)
        {
            _token = token;
        }

        public override void AddResult(Document result)
        {
            _token.Delay();

            ResultIds.Enqueue(result.Id);
        }
    }
}
