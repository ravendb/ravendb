using System;
using System.Collections.Generic;
using Jint.Native;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtlDocumentTransformer : EtlTransformer<RavenEtlItem, ICommandData>
    {
        private readonly List<ICommandData> _commands = new List<ICommandData>();
        private readonly PatchRequest _transformationScript;

        public RavenEtlDocumentTransformer(DocumentDatabase database, DocumentsOperationContext context, RavenEtlConfiguration configuration) : base(database, context)
        {
            if (string.IsNullOrEmpty(configuration.Script) == false)
                _transformationScript = new PatchRequest { Script = configuration.Script };
        }

        public override IEnumerable<ICommandData> GetTransformedResults()
        {
            return _commands;
        }

        public override void Transform(RavenEtlItem item)
        {
            if (item.IsDelete)
                _commands.Add(new DeleteCommandData(item.DocumentKey, null));
            else
            {
                if (_transformationScript != null)
                {
                    using (var scope = CreateOperationScope(Context, debugMode: false))
                    {
                        ApplySingleScript(Context, item.DocumentKey, item.Document, _transformationScript, scope);

                        var actualResult = scope.ActualPatchResult;

                        var filteredOut = false;

                        if (actualResult.IsBoolean() && actualResult.AsBoolean() == false)
                            filteredOut = true;
                        else if (actualResult.IsNull())
                            filteredOut = true;

                        if (filteredOut)
                            return;

                        JsValue result = null;

                        if (actualResult.IsObject())
                            result = actualResult;
                        else if (actualResult.IsUndefined())
                            result = scope.PatchObject.AsObject();
                        else
                            ThrowOnUnexpectedTransformationResultType(actualResult);

                        var transformResult = Context.ReadObject(scope.ToBlittable(result.AsObject()), item.DocumentKey);

                        _commands.Add(new PutCommandDataWithBlittableJson(item.DocumentKey, null, transformResult));
                    }
                }
                else
                    _commands.Add(new PutCommandDataWithBlittableJson(item.DocumentKey, null, item.Document.Data));
            }
        }

        private static void ThrowOnUnexpectedTransformationResultType(JsValue actualResult)
        {
            throw new InvalidOperationException($"Unexpected type of the transformation script result: {actualResult.Type}");
        }
    }
}