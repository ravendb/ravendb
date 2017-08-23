using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionPatchDocument
    {
        public readonly string FilterJavaScript;
        private readonly PatchRequest _patchRequest;

        public ScriptRunnerCache.Key Key => _patchRequest;

        public SubscriptionPatchDocument(DocumentDatabase database, string filterJavaScript) 
        {
            FilterJavaScript = filterJavaScript;
            _patchRequest = new PatchRequest(filterJavaScript, PatchRequestType.Subscription);
        }

        public bool MatchCriteria(ScriptRunner.SingleRun run, DocumentsOperationContext context, Document document, out BlittableJsonReaderObject transformResult)
        {
            transformResult = null;

            using (var result = run.Run(context, "execute", new object[] {document}))
            {
                if (result.Value is bool b)
                    return b;
                transformResult = result.Translate(context);
                return transformResult != null;    
            }
        }
    }
}
