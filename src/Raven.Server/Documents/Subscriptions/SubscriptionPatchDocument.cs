using Jint;
using Raven.Client.Documents.Exceptions.Patching;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionPatchDocument : DocumentPatcherBase
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

                if (result.IsNull())
                    return false; // todo: check if that is the value that we want here

                throw new JavaScriptException($"Could not proccess script {_patchRequest.Script}. It\'s return value {result.Type}, instead of bool, object or null");
            }
        }
    }
}
