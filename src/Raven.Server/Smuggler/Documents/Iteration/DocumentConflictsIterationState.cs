using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Enumerators;

namespace Raven.Server.Smuggler.Documents.Iteration
{
    public class DocumentConflictsIterationState : PulsedEnumerationState<DocumentConflict>
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
