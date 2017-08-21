using Jurassic;
using Jurassic.Library;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using JavaScriptException = Raven.Client.Exceptions.Documents.Patching.JavaScriptException;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionPatchDocument
    {
        public readonly string FilterJavaScript;
        private readonly PatchRequest _patchRequest;

        public SubscriptionPatchDocument(DocumentDatabase database, string filterJavaScript) 
        {
            FilterJavaScript = filterJavaScript;
            _patchRequest = new PatchRequest(filterJavaScript);
        }


        public bool MatchCriteria(DocumentsOperationContext context, Document document, out BlittableJsonReaderObject transformResult)
        {
            transformResult = null;
            return false;

            //using (var scope = CreateOperationScope(debugMode: false).Initialize(context))
            //{
            //    ApplySingleScript(context, document.Id, document, _patchRequest, scope);

            //    var result = scope.ActualPatchResult;

            //    if (result is bool)
            //        return (bool)result;

            //    if (result is ObjectInstance)
            //    {
            //        var transformedDynamic = scope.ToBlittable(result as ObjectInstance);
            //        transformResult = context.ReadObject(transformedDynamic, document.Id);
            //        return true;
            //    }

            //    if (result == Null.Value || result == Undefined.Value)
            //        return false; // todo: check if that is the value that we want here

            //    throw new JavaScriptException($"Could not proccess script {_patchRequest.Script}. It\'s return type is {result?.GetType()}, instead of bool, object, undefined or null");
            //}
        }
    }
}
