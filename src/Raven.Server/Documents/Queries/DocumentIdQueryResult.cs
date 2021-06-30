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
        private readonly Stopwatch _sp;

        public readonly Queue<string> DocumentIds = new Queue<string>();

        public DocumentIdQueryResult(DeterminateProgress progress, Action<DeterminateProgress> onProgress, OperationCancelToken token)
        {
            _progress = progress;
            _onProgress = onProgress;
            _token = token;
            _sp = Stopwatch.StartNew();
        }

        public override void AddResult(Document result)
        {
            using (result)
            {
                _token.Delay();
                DocumentIds.Enqueue(result.Id);

                _progress.Total++;

                if (_sp.ElapsedMilliseconds > 1000)
                {
                    _onProgress.Invoke(_progress);
                    _sp.Restart();
                }
            }
        }
    }
}
