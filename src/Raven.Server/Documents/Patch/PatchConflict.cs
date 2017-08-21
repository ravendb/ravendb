using System;
using System.Collections.Generic;
using Jurassic;
using Jurassic.Library;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public class PatchConflict : DocumentPatcherBase
    {
        private readonly List<DocumentConflict> _docs = new List<DocumentConflict>();
        private readonly bool _hasTombstone;
        private static readonly string TombstoneResolverValue = Guid.NewGuid().ToString();
        private readonly int _docsSize;

        public PatchConflict(DocumentDatabase database, IReadOnlyCollection<DocumentConflict> docs) : base(database)
        {
            foreach (var doc in docs)
            {
                if (doc.Doc != null)
                {
                    _docsSize += doc.Doc.Size;
                    _docs.Add(doc);
                }
                else
                {
                    _hasTombstone = true;
                }

            }
            ExecutionString = @"function ExecutePatchScript(docs){{ {0} }}";
        }

        public bool TryResolveConflict(DocumentsOperationContext context, PatchRequest patch, out BlittableJsonReaderObject resolved)
        {
            using (var scope = CreateOperationScope(debugMode: false).Initialize(context))
            {
                var run = new SingleScriptRun(this, patch, scope);
                try
                {
                    run.Prepare(_docsSize);
                    SetupInputs(scope, run.JSEngine);
                    run.Execute();

                    return TryParse(context, scope, out resolved);
                }
                catch (Exception errorEx)
                {
                    run.HandleError(errorEx);
                    throw;
                }
            }
        }

        protected void SetupInputs(PatcherOperationScope scope, ScriptEngine jintEngine)
        {
            var docsArr = jintEngine.Array.Construct();
            
            for (var i = 0; i < _docs.Count; i++)
            {
                var doc = _docs[i];
//TODO : add unit test that has a conflict here to make sure that it is ok
                var jsVal = scope.ToJsObject(jintEngine, doc, "doc" + i);
                docsArr.Push((object)jsVal);
            
            }
            // todo: don't think we need this
            //docsArr.DefineProperty("length",  new PropertyDescriptor
            //{
            //    Value = _docs.Count,
            //});

            scope.PatchObject = docsArr;
        }

        protected override void CustomizeEngine(ScriptEngine engine, PatcherOperationScope scope)
        {
            engine.SetGlobalFunction("ResolveToTombstone", new Func<string>(() => TombstoneResolverValue));
            engine.SetGlobalValue("HasTombstone", _hasTombstone);
        }

        protected override void RemoveEngineCustomizations(ScriptEngine engine, PatcherOperationScope scope)
        {
            engine.Global.Delete("ResolveToTombstone", false);
            engine.Global.Delete(TombstoneResolverValue, false);
            engine.Global.Delete("HasTombstone", false);
        }

        private bool TryParse(JsonOperationContext context, PatcherOperationScope scope, out BlittableJsonReaderObject val)
        {
            if (scope.ActualPatchResult == Undefined.Value || scope.ActualPatchResult == Null.Value)
            {
                val = null;
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info($"Conflict resolution script for {_docs[0].Collection} collection declined to resolve the conflict for {_docs[0].LowerId}");
                }
                return false;
            }

            if (scope.ActualPatchResult == TombstoneResolverValue)
            {
                val = null;
                return true;
            }
            var obj = scope.ActualPatchResult as ObjectInstance;
            using (var writer = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
            {
                writer.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                writer.StartWriteObjectDocument();
                writer.StartWriteObject();
                var resolvedMetadata = obj.GetPropertyValue(Constants.Documents.Metadata.Key);
                if (resolvedMetadata == null ||
                    resolvedMetadata == Undefined.Value)
                {
                    // if user didn't specify it, we'll take it from the first doc
                    foreach (var doc in _docs)
                    {
                        if (doc.Doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
                        {
                            continue;
                        }

                        writer.WritePropertyName(Constants.Documents.Metadata.Key);
                        writer.StartWriteObject();

                        var prop = new BlittableJsonReaderObject.PropertyDetails();
                        for (int i = 0; i < metadata.Count; i++)
                        {
                            metadata.GetPropertyByIndex(i, ref prop);
                            writer.WritePropertyName(prop.Name.ToString());
                            switch (prop.Token & BlittableJsonReaderBase.TypesMask)
                            {
                                case BlittableJsonToken.StartObject:
                                case BlittableJsonToken.StartArray:
                                case BlittableJsonToken.EmbeddedBlittable:
                                    // explicitly ignoring those, if user have
                                    // such objects in metadata, they need to 
                                    // manually merge it
                                    break;

                                case BlittableJsonToken.Integer:
                                    writer.WriteValue((long)prop.Value);
                                    break;
                                case BlittableJsonToken.LazyNumber:
                                    writer.WriteValue((double)prop.Value);
                                    break;
                                case BlittableJsonToken.CompressedString:
                                case BlittableJsonToken.String:
                                    writer.WriteValue(prop.Value.ToString());
                                    break;
                                case BlittableJsonToken.Boolean:
                                    writer.WriteValue((bool)prop.Value);
                                    break;
                                case BlittableJsonToken.Null:
                                    writer.WriteValueNull();
                                    break;
                            }
                        }

                        writer.WriteObjectEnd();

                        break;
                    }
                }

                scope.WriteRawObjectPropertiesToBlittable(writer, obj);
                writer.WriteObjectEnd();
                writer.FinalizeDocument();
                val = writer.CreateReader();
                return true;
            }
        }
    }
}

