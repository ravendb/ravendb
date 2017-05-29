using System;
using Jint;
using Jint.Native;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents
{
    public class SmugglerPatcher
    {
        private readonly Engine _engine;

        public SmugglerPatcher(DatabaseSmugglerOptions options)
        {
            if (string.IsNullOrWhiteSpace(options.TransformScript))
                throw new InvalidOperationException("Cannot create a patcher with empty transform script.");

            _engine = new Engine(cfg =>
            {
                cfg.AllowDebuggerStatement(false);
                cfg.MaxStatements(options.MaxStepsForTransformScript);
                cfg.NullPropagation();
            });

            _engine.Execute(string.Format(@"
                    function Transform(docInner){{
                        return ({0}).apply(this, [docInner]);
                    }};", options.TransformScript));
        }

        public Document Transform(Document document, JsonOperationContext context)
        {
            _engine.ResetStatementsCount();

            using (var scope = new OperationScope())
            {
                var jsObject = scope.ToJsObject(_engine, document);
                var jsObjectTransformed = _engine.Invoke("Transform", jsObject);

                if (jsObjectTransformed.IsObject() == false)
                {
                    document.Data.Dispose();
                    return null;
                }

                var newDocument = context.ReadObject(scope.ToBlittable(jsObjectTransformed.AsObject()), document.Id);
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

            public override JsValue LoadDocument(string documentId, Engine engine, ref int totalStatements)
            {
                throw new NotSupportedException("LoadDocument is not supported.");
            }

            public override string PutDocument(string id, JsValue document, JsValue metadata, JsValue etagJs, Engine engine)
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