using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Smuggler.Documents.Iteration
{
    public class DocumentConflictsIterationState : IterationState<DocumentConflict>
    {
        public DocumentConflictsIterationState(DocumentsOperationContext context) : base(context)
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
