using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Client;
using Sparrow.Json;
using Sparrow.Utils;
using Raven.Client.ServerWide.JavaScript;
using PatchJint = Raven.Server.Documents.Patch.Jint;
using PatchV8 = Raven.Server.Documents.Patch.V8;
using Jint;
using V8.Net;

namespace Raven.Server.Documents.Patch
{
    public class JsBlittableBridge
    {
        protected readonly ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> _writer;
        protected readonly BlittableJsonDocumentBuilder.UsageMode _usageMode;

        [ThreadStatic]
        protected static HashSet<object> _recursive;

        [ThreadStatic]
        protected static uint _recursiveNativeObjectsCount;

        protected static readonly double MaxJsDateMs = (DateTime.MaxValue - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
        protected static readonly double MinJsDateMs = -(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc) - DateTime.MinValue).TotalMilliseconds;

        static JsBlittableBridge()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += () =>
            {
                _recursive = null;
                _recursiveNativeObjectsCount = 0;
            };
        }

        public JsBlittableBridge(ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> writer, BlittableJsonDocumentBuilder.UsageMode usageMode)
        {
            _writer = writer;
            _usageMode = usageMode;
        }

        public static BlittableJsonReaderObject Translate(JsonOperationContext context, IJsEngineHandle engine, JsHandle objectInstance, 
            IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None, 
            bool isRoot = true)
        {
            var jsEngineType = engine.EngineType;
            return  jsEngineType switch
            {
                JavaScriptEngineType.Jint => PatchJint.JsBlittableBridgeJint.Translate(context, (Engine)engine, objectInstance.Jint.Obj, modifier, usageMode, isRoot),
                JavaScriptEngineType.V8 => PatchV8.JsBlittableBridgeV8.Translate(context, (V8Engine)engine, objectInstance.V8.Item, modifier, usageMode, isRoot),
                _ => throw new NotSupportedException($"Not supported JS engine kind '{jsEngineType}'.")
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void WriteObjectType(object target)
        {
            _writer.WriteValue('[' + target.GetType().Name + ']');
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static bool ShouldFilterProperty(bool filterProperties, string property)
        {
            if (filterProperties == false)
                return false;

            return property == Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName ||
                   property == Constants.Documents.Indexing.Fields.DocumentIdFieldName ||
                   property == Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName ||
                   property == Constants.Documents.Metadata.Id ||
                   property == Constants.Documents.Metadata.LastModified ||
                   property == Constants.Documents.Metadata.IndexScore ||
                   property == Constants.Documents.Metadata.ChangeVector ||
                   property == Constants.Documents.Metadata.Flags;
        }

        public interface IResultModifier
        {
            void Modify(JsHandle json);
        }
    }
}
