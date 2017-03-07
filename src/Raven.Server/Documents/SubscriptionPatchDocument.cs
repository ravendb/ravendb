using Jint;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents
{
    public class SubscriptionPatchDocument : PatchDocument
    {
        private readonly PatchRequest _patchRequest;

        public SubscriptionPatchDocument(DocumentDatabase database, string filterJavaScript) : base(database)
        {
            _patchRequest = new PatchRequest
            {
                Script = filterJavaScript
            };
        }

        protected override void CustomizeEngine(Engine engine, PatcherOperationScope scope)
        {
            // override to make it "no-op"
        }

        public bool MatchCriteria(DocumentsOperationContext context, Document document)
        {
            var actualPatchResult = ApplySingleScript(context, document, _patchRequest, debugMode: false).ActualPatchResult;
            return actualPatchResult.AsBoolean();
        }
    }
}
