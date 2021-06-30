using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Queries
{
    public class DocumentIdQueryResult : DocumentQueryResult
    {
        private readonly DeterminateProgress _progress;
        private readonly Action<DeterminateProgress> _onProgress;
        private readonly OperationCancelToken _token;

        public readonly Queue<string> DocumentIds = new Queue<string>();

        public DocumentIdQueryResult(DeterminateProgress progress, Action<DeterminateProgress> onProgress, OperationCancelToken token)
        {
            _progress = progress;
            _onProgress = onProgress;
            _token = token;
        }

        public override void AddResult(Document result)
        {
            using (result)
            {
                _token.Delay();
                DocumentIds.Enqueue(result.Id);

                _progress.Total++;

                if (_progress.Total % 10_000 == 0)
                {
                    _onProgress.Invoke(_progress);
                }
            }
        }
    }
}
