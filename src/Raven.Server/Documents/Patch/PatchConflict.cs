using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using Jint;
using Jint.Native;
using Jint.Parser;
using Jint.Parser.Ast;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Raven.Client.Replication.Messages;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Data.Tables;
using Voron.Exceptions;
using TypeExtensions = System.Reflection.TypeExtensions;

namespace Raven.Server.Documents.Patch
{
    public class PatchConflict : PatchDocument
    {
        private readonly List<DocumentConflict> _docs = new List<DocumentConflict>();
        private readonly string _key;
        private readonly bool _hasTombstone = false;
        private const string TombstoneResolverValue = "_To_Tombstone__";

        public PatchConflict(DocumentDatabase database, IReadOnlyCollection<DocumentConflict> docs,
              string key):base(database)
        {
            foreach (var doc in docs)
            {
                if (doc.Doc != null)
                {
                    _docs.Add(doc);
                }
            }
            _hasTombstone = docs.Any(doc => doc.Doc == null);
            ExecutionString = @"function ExecutePatchScript(docs){{ {0} }}";
            _key = key;     
        }

        public override PatchResultData Apply(DocumentsOperationContext context, Document document, PatchRequest patch)
        {
            if (string.IsNullOrEmpty(patch.Script))
                throw new InvalidOperationException("Patch script must be non-null and not empty");

            var scope = ApplySingleScript(context, document, false, patch);

            var resolvedDocument = TryParse(context, scope);
            return new PatchResultData
            {
                ModifiedDocument = resolvedDocument,
                DebugInfo = scope.DebugInfo
            };
        }

        protected override void SetupInputs(Document document, PatcherOperationScope scope, Engine jintEngine)
        {
            var docsArr = jintEngine.Array.Construct(Arguments.Empty);
            int index = 0;
            foreach (var doc in _docs)
            {
                //TODO : add unit test that has a conflict here to make sure that it is ok
                var jsVal = scope.ToJsObject(jintEngine, doc.Doc, "doc" + index);
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
            engine.SetValue("ResolveToTombstone", new Func<string>(()=> TombstoneResolverValue));

            engine.Global.Delete("HasTombstone", false);
            engine.SetValue("HasTombstone", _hasTombstone);
        }

        private BlittableJsonReaderObject TryParse(DocumentsOperationContext context, PatcherOperationScope scope)
        {
            if (scope.ActualPatchResult == JsValue.Undefined || scope.ActualPatchResult == JsValue.Undefined)
            {
                throw new OperationCanceledException
                    ("It seems that the script was unable to resolve the conflict");
            }

            if (scope.ActualPatchResult == TombstoneResolverValue)
            {
                // getting a Function instance here,
                // means that we couldn't evaluate it using Jint
                return null;
            }
            var obj = scope.ActualPatchResult.AsObject();
            using (var writer = new ManualBlittalbeJsonDocumentBuilder(context))
            {
                writer.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                writer.StartWriteObjectDocument();
                scope.ToBlittableJsonReaderObject(writer, obj);
                writer.FinalizeDocument();
                return writer.CreateReader();
            }
        }
    }
}

