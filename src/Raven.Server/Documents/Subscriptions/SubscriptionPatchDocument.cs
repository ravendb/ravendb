using System;
using Jint;
using Jint.Native;
using Jint.Native.Function;
using Jint.Runtime.Environments;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Subscriptions
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

        public bool MatchCriteria(DocumentsOperationContext context, Document document, out BlittableJsonReaderObject transformResult)
        {
            var patchingScope = GenerateDefaultOperationScope(context, false);

            transformResult = null;

            var externalScope = new PatcherOperationScope(this._database,context);
            var actualPatchResult = ApplySingleScript(context, document, _patchRequest, false, externalScope).ActualPatchResult;

            if (actualPatchResult.IsBoolean())
                return actualPatchResult.AsBoolean();

            if (actualPatchResult.IsObject())
            {
                var transformedDynamic = externalScope.ToBlittable(actualPatchResult.AsObject());
                transformResult = context.ReadObject(transformedDynamic, document.Key);
                return true;
            }

            if (actualPatchResult.IsNull())
                return false; // todo: check if that is the value that we want here

            throw new ArgumentException(
                $"Could not proccess script {_patchRequest.Script}. It\'s return value {actualPatchResult.Type}, instead of bool, object or null");
        }
    }
}
