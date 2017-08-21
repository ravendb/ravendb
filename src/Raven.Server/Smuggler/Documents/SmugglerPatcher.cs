using System;
using Jurassic;
using Jurassic.Library;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents
{
    public class SmugglerPatcher
    {
        private readonly ScriptEngine _engine;

        public SmugglerPatcher(DatabaseSmugglerOptions options)
        {
            if (string.IsNullOrWhiteSpace(options.TransformScript))
                throw new InvalidOperationException("Cannot create a patcher with empty transform script.");
           
            _engine = new ScriptEngine();

#if DEBUG
            _engine.EnableDebugging = true;
#endif

            _engine.RecursionDepthLimit = DocumentPatcherBase.MaxRecursionDepth;
            _engine.OnLoopIterationCall = new DocumentPatcherBase.EngineLoopIterationKeeper(options.MaxStepsForTransformScript).OnLoopIteration;

            _engine.Execute(string.Format(@"
                    function Transform(docInner){{
                        return ({0}).apply(this, [docInner]);
                    }};", options.TransformScript));
        }

        public Document Transform(Document document, JsonOperationContext context)
        {
            var keeper = _engine.OnLoopIterationCallTarget as DocumentPatcherBase.EngineLoopIterationKeeper;
            if (keeper != null)
                keeper.LoopIterations = 0;

            using (var scope = new OperationScope())
            {
                var jsObject = scope.ToJsObject(_engine, document);
                var jsObjectTransformed = _engine.CallGlobalFunction("Transform", jsObject);

                if (jsObjectTransformed is ObjectInstance == false)
                {
                    document.Data.Dispose();
                    return null;
                }

                var newDocument = context.ReadObject(scope.ToBlittable(jsObjectTransformed as ObjectInstance), document.Id);
                if (newDocument.Equals(document.Data))
                {
                    newDocument.Dispose();
                    return document;
                }

                document.Data.Dispose();

                return new Document
                {
                    Data = newDocument,
                    Id = document.Id,
                    Flags = document.Flags,
                    NonPersistentFlags = document.NonPersistentFlags
                };
            }
        }

        private class OperationScope : PatcherOperationScope
        {
            public OperationScope()
                : base(null)
            {
            }

            public override object LoadDocument(string documentId, ScriptEngine engine)
            {
                throw new NotSupportedException("LoadDocument is not supported.");
            }

            public override string PutDocument(string id, object document, object metadata, string changeVector, ScriptEngine engine)
            {
                throw new NotSupportedException("PutDocument is not supported.");
            }

            public override void DeleteDocument(string documentId)
            {
                throw new NotSupportedException("DeleteDocument is not supported.");
            }
        }
    }
}
