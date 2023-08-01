using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Enumerators;
using Sparrow;

namespace Raven.Server.Smuggler.Documents.Iteration
{
    public sealed class DocumentConflictsIterationState : PulsedEnumerationState<DocumentConflict>
    {
        public DocumentConflictsIterationState(DocumentsOperationContext context, Size pulseLimit) : base(context, pulseLimit)
        {
        }

        public long StartEtag;

        public override void OnMoveNext(DocumentConflict current)
        {
            StartEtag = current.Etag + 1;

            ReadCount++;
        }
    }
}
