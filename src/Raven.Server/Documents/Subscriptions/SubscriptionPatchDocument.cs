using Jint;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionPatchDocument : DocumentPatcherBase
    {
        public readonly string FilterJavaScript;
        private readonly PatchRequest _patchRequest;

        public SubscriptionPatchDocument(DocumentDatabase database, string filterJavaScript) : base(database)
        {
            FilterJavaScript = filterJavaScript;
            _patchRequest = new PatchRequest
            {
                Script = filterJavaScript
            };
        }

        protected override void CustomizeEngine(Engine engine, PatcherOperationScope scope)
        {

        }

        protected override void RemoveEngineCustomizations(Engine engine, PatcherOperationScope scope)
        {
            
        }

        public bool MatchCriteria(DocumentsOperationContext context, Document document, out BlittableJsonReaderObject transformResult)
        {
            transformResult = null;

            using (var scope = CreateOperationScope(debugMode: false).Initialize(context))
            {
                ApplySingleScript(context, document.Id, document, _patchRequest, scope);

                var result = scope.ActualPatchResult;

                if (result.IsBoolean())
                    return result.AsBoolean();

                if (result.IsObject())
                {
                    var transformedDynamic = scope.ToBlittable(result.AsObject());
                    transformResult = context.ReadObject(transformedDynamic, document.Id);
                    return true;
                }

                if (result.IsNull() || result.IsUndefined())
                    return false; // todo: check if that is the value that we want here

                throw new JavaScriptException($"Could not proccess script {_patchRequest.Script}. It\'s return value {result.Type}, instead of bool, object, undefined or null");
            }
        }
    }
}
