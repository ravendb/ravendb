using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
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
            using (var scope = CreateOperationScope(context, debugMode: false))
            {
                var run = new SingleScriptRun(this, patch, scope);
                try
                {
                    run.Prepare(_docsSize);
                    SetupInputs(scope, run.JintEngine);
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

        protected void SetupInputs(PatcherOperationScope scope, Engine jintEngine)
        {
            var docsArr = jintEngine.Array.Construct(Arguments.Empty);
            int index = 0;
            foreach (var doc in _docs)
            {
                //TODO : add unit test that has a conflict here to make sure that it is ok
                var jsVal = scope.ToJsObject(jintEngine, doc, "doc" + index);
                docsArr.FastAddProperty(index.ToString(), jsVal, true, true, true);
                index++;
            }
            docsArr.FastSetProperty("length", new PropertyDescriptor
            {
                Value = new JsValue(index),
                Configurable = true,
                Enumerable = true,
                Writable = true,
            });

            scope.PatchObject = docsArr;
        }

        protected override void CustomizeEngine(Engine engine, PatcherOperationScope scope)
        {
            base.CustomizeEngine(engine, scope);

            engine.Global.Delete("ResolveToTombstone", false);
            engine.Global.Delete(TombstoneResolverValue, false);
            engine.SetValue("ResolveToTombstone", new Func<string>(() => TombstoneResolverValue));

            engine.Global.Delete("HasTombstone", false);
            engine.SetValue("HasTombstone", _hasTombstone);
        }

        private bool TryParse(DocumentsOperationContext context, PatcherOperationScope scope, out BlittableJsonReaderObject val)
        {
            if (scope.ActualPatchResult == JsValue.Undefined || scope.ActualPatchResult == JsValue.Undefined)
            {
                val = null;
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info($"Conflict resolution script for {_docs[0].Collection} collection declined to resolve the conflict for {_docs[0].LoweredKey}");
                }
                return false;
            }

            if (scope.ActualPatchResult == TombstoneResolverValue)
            {
                val = null;
                return true;
            }
            var obj = scope.ActualPatchResult.AsObject();
            using (var writer = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
            {
                writer.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                writer.StartWriteObjectDocument();
                writer.StartWriteObject();
                var resolvedMetadata = obj.Get(Constants.Documents.Metadata.Key);
                if (resolvedMetadata == null ||
                    resolvedMetadata == JsValue.Undefined)
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
                                case BlittableJsonToken.Float:
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

