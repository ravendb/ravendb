using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Esprima.Ast;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Function;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Documents.Indexes.Static.Utils;
using JintPreventResolvingTasksReferenceResolver = Raven.Server.Documents.Patch.Jint.JintPreventResolvingTasksReferenceResolver;
using V8Exception = V8.Net.V8Exception;
using JavaScriptException = Jint.Runtime.JavaScriptException;

namespace Raven.Server.Documents.Indexes.Static
{
    public partial class JavaScriptReduceOperation
    {
        private readonly AbstractJavaScriptIndex _index;
        private JavaScriptIndexUtils _jsIndexUtils { get; }
        private IJavaScriptUtils _jsUtils { get; }
        private IJsEngineHandle EngineHandle { get; }
        private JavaScriptEngineType _jsEngineType => EngineHandle.EngineType;
        private IJavaScriptEngineForParsing EngineForParsing { get; }
        public ScriptFunctionInstance KeyJint { get; }

        public JsHandle Reduce { get; }
        public JsHandle Key { get; }

        protected Dictionary<BlittableJsonReaderObject, List<BlittableJsonReaderObject>> _groupedItems;

        private readonly long _indexVersion;
        
        public JavaScriptReduceOperation(AbstractJavaScriptIndex index, JavaScriptIndexUtils jsIndexUtils, ScriptFunctionInstance keyJint, IJavaScriptEngineForParsing engineForParsing,
            JsHandle reduce, JsHandle key, long indexVersion)
        {
            _index = index;
            _indexVersion = indexVersion;
            EngineHandle = jsIndexUtils.EngineHandle;
            _groupedItems = null;

            KeyJint = keyJint ?? throw new ArgumentNullException(nameof(keyJint));
            EngineForParsing = engineForParsing;
            GetReduceFieldsNames();

            if (reduce.IsUndefined || reduce.IsNull)
                throw new ArgumentNullException(nameof(reduce));
            Reduce = new JsHandle(ref reduce);

            if (key.IsUndefined || key.IsNull)
                throw new ArgumentNullException(nameof(key));
            Key = new JsHandle(ref key);

            _jsIndexUtils = jsIndexUtils;
            _jsUtils = _jsIndexUtils.JsUtils;
        }

        ~JavaScriptReduceOperation()
        {
            Reduce.Dispose();
            Key.Dispose();
        }

        protected struct GroupByKeyComparer : IEqualityComparer<BlittableJsonReaderObject>
        {
            private readonly JavaScriptReduceOperation _parent;
            private readonly ReduceKeyProcessor _xKey;
            private readonly ReduceKeyProcessor _yKey;
            private BlittableJsonReaderObject _lastUsedBlittable;
            private BlittableJsonReaderObject _lastUsedBucket;
            private readonly ByteStringContext _allocator;

            public GroupByKeyComparer(JavaScriptReduceOperation parent, UnmanagedBuffersPoolWithLowMemoryHandling buffersPool, ByteStringContext allocator, long indexVersion)
            {
                _parent = parent;
                _allocator = allocator;
                _xKey = new ReduceKeyProcessor(_parent._groupByFields.Length, buffersPool, indexVersion);
                _yKey = new ReduceKeyProcessor(_parent._groupByFields.Length, buffersPool, indexVersion);
                _xKey.SetMode(ReduceKeyProcessor.Mode.MultipleValues);
                _yKey.SetMode(ReduceKeyProcessor.Mode.MultipleValues);
                _lastUsedBlittable = null;
                _lastUsedBucket = null;
            }

            public unsafe bool Equals(BlittableJsonReaderObject x, BlittableJsonReaderObject y)
            {
                var xCalculated = ReferenceEquals(x, _lastUsedBucket);
                //Only y is calculated, x is the value in the bucket
                var yCalculated = ReferenceEquals(y, _lastUsedBlittable);
                if (xCalculated == false)
                    _xKey.Reset();
                if (yCalculated == false)
                    _yKey.Reset();

                foreach (var field in _parent._groupByFields)
                {
                    bool xHasField = false;
                    bool yHasField = false;
                    object xVal = null;
                    if (xCalculated == false)
                        xHasField = x.TryGet(field.Name, out xVal);

                    object yVal = null;
                    if (yCalculated == false && xCalculated == false)
                    {
                        yHasField = y.TryGet(field.Name, out yVal);
                        if (xHasField != yHasField)
                            return false;
                    }

                    if (xCalculated == false)
                    {
                        if (xHasField)
                            xVal = field.GetValue(null, xVal);

                        _xKey.Process(_allocator, xVal);
                    }

                    if (yCalculated == false)
                    {
                        if (yHasField)
                            yVal = field.GetValue(null, yVal);

                        _yKey.Process(_allocator, yVal);
                    }
                }

                var xIsNotAllNulls = _xKey.IsBufferSet;
                var yIsNotAllNulls = _yKey.IsBufferSet;
                // null == null
                if (xIsNotAllNulls == false && yIsNotAllNulls == false)
                    return true;
                // x == null and y != null or y != null and y == null
                if (xIsNotAllNulls == false || yIsNotAllNulls == false)
                    return false;

                //At this point both buffer should be populated
                var xBuffer = _xKey.GetBuffer();
                var yBuffer = _yKey.GetBuffer();
                _lastUsedBucket = x;
                if (xBuffer.Size != yBuffer.Size)
                    return false;

                return Memory.Compare(xBuffer.Address, yBuffer.Address, xBuffer.Size) == 0;
            }

            public int GetHashCode(BlittableJsonReaderObject obj)
            {
                _yKey.Reset();
                foreach (var field in _parent._groupByFields)
                {
                    if (obj.TryGet(field.Name, out object xVal))
                        xVal = field.GetValue(null, xVal);

                    _yKey.Process(_allocator, xVal);
                }

                _lastUsedBlittable = obj;

                return (int)Hashing.Mix(_yKey.Hash);
            }
        }

        public IEnumerable IndexingFunction(IEnumerable<dynamic> items)
        {
            try
            {
                EnsureGroupItemCreated();
                foreach (DynamicBlittableJson item in items)
                {
                    if (_groupedItems.TryGetValue(item.BlittableJson, out var list) == false)
                    {
                        list = new List<BlittableJsonReaderObject>();
                        _groupedItems[item.BlittableJson] = list;
                    }

                    list.Add(item.BlittableJson);
                }

                lock (EngineHandle)
                {
                    switch (EngineHandle.EngineType)
                    {
                        case JavaScriptEngineType.Jint:
                            SetContextJint();
                            break;
                        case JavaScriptEngineType.V8:
                            SetContextV8();
                            break;
                        default:
                            throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.");
                    }

                    var memorySnapshotName = "reduce";
                    bool isMemorySnapshotMade = false;
                    if (EngineHandle.IsMemoryChecksOn)
                    {
                        EngineHandle.MakeSnapshot(memorySnapshotName);
                        isMemorySnapshotMade = true;
                    }

                    foreach (var item in _groupedItems.Values)
                    {
                        _index._lastException = null;

                        EngineHandle.ResetCallStack();
                        EngineHandle.ResetConstraints();

                        JsHandle jsRes = JsHandle.Empty;
                        try
                        {
                            using (var jsGrouping = ConstructGrouping(item))
                            {
                                jsRes = Reduce.StaticCall(jsGrouping);
                                if (_index._lastException != null)
                                {
                                    ExceptionDispatchInfo.Capture(_index._lastException).Throw();
                                }
                                else
                                {
                                    jsRes.ThrowOnError();
                                }

                                if (jsRes.IsObject == false)
                                    throw new JavaScriptIndexFuncException($"Failed to execute {ReduceString}",
                                        new Exception($"Reduce result is not object: {jsRes.ToString()}"));
                            }
                        }
                        catch (V8Exception jse)
                        {
                            ProcessRunException(jsRes, memorySnapshotName, isMemorySnapshotMade);
                            var (message, success) = JavaScriptIndexFuncException.PrepareErrorMessageForJavaScriptIndexFuncException(ReduceString, jse);
                            if (success == false)
                                throw new JavaScriptIndexFuncException($"Failed to execute {ReduceString}", jse);
                            throw new JavaScriptIndexFuncException($"Failed to execute reduce script, {message}", jse);
                        }
                        catch (Exception e)
                        {
                            ProcessRunException(jsRes, memorySnapshotName, isMemorySnapshotMade);
                            throw new JavaScriptIndexFuncException($"Failed to execute {ReduceString}", e);
                        }
                        finally
                        {
                            _index._lastException = null;
                        }

                        if (isMemorySnapshotMade)
                        {
                            EngineHandle.AddToLastMemorySnapshotBefore(jsRes);
                        }

                        yield return jsRes;

                        EngineHandle.ForceGarbageCollection();
                        if (isMemorySnapshotMade)
                        {
                            EngineHandle.CheckForMemoryLeaks(memorySnapshotName, shouldRemove: false);
                        }
                    }
                    // memory snapshot is removed after removing all reduce results and the final check in AggregatedAnonymousObjects.Dispose() 
                }
            }
            finally
            {
                _groupedItems.Clear();
            }
        }


        private void ProcessRunException(JsHandle jsRes, string memorySnapshotName, bool isMemorySnapshotMade)
        {
            EngineHandle.AddToLastMemorySnapshotBefore(jsRes); // as jsRes has been saved in V8Exception
            jsRes.Dispose(); // jsRes still has one reference

            EngineHandle.ForceGarbageCollection();
            if (isMemorySnapshotMade)
            {
                EngineHandle.CheckForMemoryLeaks(memorySnapshotName, shouldRemove: false);
            }
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void EnsureGroupItemCreated()
        {
            if (_groupedItems == null)
            {
                if (_bufferPool == null)
                {
                    _bufferPool = CurrentIndexingScope.Current.UnmanagedBuffersPool;
                }

                if (_byteStringContext == null)
                {
                    _byteStringContext = CurrentIndexingScope.Current.IndexContext.Allocator;
                }

                _groupedItems = new Dictionary<BlittableJsonReaderObject, List<BlittableJsonReaderObject>>(new GroupByKeyComparer(this, _bufferPool, _byteStringContext, _indexVersion));
            }
        }

        private JsHandle ConstructGrouping(List<BlittableJsonReaderObject> values)
        {
            var result = EngineHandle.CreateObject();
            result.SetProperty("values", ConstructValues());
            result.SetProperty("key", ConstructKey());

            return result;

            JsHandle ConstructKey()
            {
                if (_singleField)
                {
                    var index = values[0].GetPropertyIndex(_groupByFields[0].Name);
                    if (index != -1)
                    {
                        BlittableJsonReaderObject.PropertyDetails prop = default;
                        values[0].GetPropertyByIndex(index, ref prop);
                        return _jsIndexUtils.GetValueOrThrow(prop.Value, isMapReduce: true);
                    }

                    return EngineHandle.CreateNullValue();
                }

                JsHandle jsRes;
                jsRes = EngineHandle.CreateObject();
                foreach (var groupByField in _groupByFields)
                {
                    var index = values[0].GetPropertyIndex(groupByField.Name);
                    if (index != -1)
                    {
                        BlittableJsonReaderObject.PropertyDetails prop = default;
                        values[0].GetPropertyByIndex(index, ref prop);

                        var propertyName = groupByField.Name;
                        if (groupByField is JsNestedField jsnf)
                            propertyName = jsnf.PropertyName;

                        var value = groupByField.GetValue(null, prop.Value);
                        var jsValue = value switch
                        {
                            BlittableJsonReaderObject bjro => ((Func<BlittableJsonReaderObject, JsHandle>)((BlittableJsonReaderObject bjro) =>
                            {
                                var boi = _jsUtils.CreateBlittableObjectInstanceFromScratch(_jsUtils, null, bjro, null, null, null);
                                return boi.CreateJsHandle(true);
                            }))(bjro),
                            Document doc => ((Func<Document, JsHandle>)((Document doc) =>
                            {
                                var boi = _jsUtils.CreateBlittableObjectInstanceFromDoc(_jsUtils, null, doc.Data, doc);
                                return boi.CreateJsHandle(true);
                            }))(doc),
                            LazyNumberValue lnv => EngineHandle.CreateValue(lnv.ToDouble(CultureInfo.InvariantCulture)),
                            _ => _jsIndexUtils.GetValueOrThrow(value, isMapReduce: true)
                        };

                        jsRes.SetProperty(propertyName, jsValue);
                    }
                }

                return jsRes;
            }

            JsHandle ConstructValues()
            {
                int arrayLength = values.Count;
                var jsItems = new JsHandle[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    var val = values[i];

                    if (_jsIndexUtils.GetValue(val, out JsHandle jsValueHandle, isMapReduce: true) == false)
                        continue;

                    jsItems[i] = jsValueHandle;
                }

                return EngineHandle.CreateArray(jsItems);
            }
        }
        
        public string ReduceString { get; internal set; }

        protected CompiledIndexField[] _groupByFields;
        protected bool _singleField;
        private UnmanagedBuffersPoolWithLowMemoryHandling _bufferPool;
        private ByteStringContext _byteStringContext;

        internal CompiledIndexField[] GetReduceFieldsNames()
        {
            if (_groupByFields != null)
                return _groupByFields;

            var ast = KeyJint.FunctionDeclaration;
            var body = ast.ChildNodes.ToList();

            if (body.Count != 2)
            {
                throw new InvalidOperationException($"Was requested to get reduce fields from a scripted function in an unexpected format, expected a single return statement got {body.Count}.");
            }

            var parameters = ast.Params;
            if (parameters.Count != 1)
            {
                throw new InvalidOperationException($"Was requested to get reduce fields from a scripted function in an unexpected format, expected a single argument but got {parameters.Count}.");
            }

            if (parameters[0] is Identifier == false)
            {
                throw new InvalidOperationException($"Was requested to get reduce fields from a scripted function in an unexpected format, expected a single argument of type 'Identifier' but got {parameters[0].GetType().Name}.");
            }

            var actualBody = body[1];
            switch (actualBody)
            {
                // x => {
                //   return {
                //     A: x.A,
                //     B: x.B
                //   }
                // }
                case BlockStatement bs:
                    var blockBody = bs.Body;

                    if (blockBody.Count == 1 && blockBody[0] is ReturnStatement returnStmt)
                    {
                        if (returnStmt.ChildNodes.Count == 1 && returnStmt.ChildNodes[0] is ObjectExpression returnObjectExpression)
                        {
                            return _groupByFields = CreateFieldsFromObjectExpression(returnObjectExpression);
                        }
                    }
                    throw new InvalidOperationException($"Expected statement returning simple object expression inside group by block");

                // x => x.Name
                case StaticMemberExpression sme:
                    if (sme.Property is Identifier id)
                    {
                        _groupByFields = new[] { CreateField(id.Name, GetPropertyPath(sme).ToArray()) };
                        _singleField = true;

                        return _groupByFields;
                    }

                    throw new InvalidOperationException($"Was requested to get reduce fields from a scripted function in an unexpected format, expected a single return object expression statement got a statement of type {actualBody.GetType().Name}.");

                // x => ({ A: x.A, B: x.B })
                case ObjectExpression oe:
                    _groupByFields = CreateFieldsFromObjectExpression(oe);
                    return _groupByFields;

                default:
                    throw new InvalidOperationException($"Unknown body type: {actualBody.GetType().Name}");
            }

            CompiledIndexField[] CreateFieldsFromObjectExpression(ObjectExpression oe)
            {
                var cur = new HashSet<CompiledIndexField>();
                foreach (var prop in oe.Properties)
                {
                    if (prop is Property property)
                    {
                        string[] path = null;
                        if (property.Value is MemberExpression me)
                            path = GetPropertyPath(me).ToArray();

                        var propertyName = property.GetKey((Engine)EngineForParsing);
                        cur.Add(CreateField(propertyName.AsString(), path));
                    }
                }

                return cur.ToArray();
            }

            CompiledIndexField CreateField(string propertyName, string[] path)
            {
                if (path == null || path.Length <= 1)
                    return new SimpleField(propertyName);

                return new JsNestedField(propertyName, path[0], path.Skip(1).ToArray());
            }

            IEnumerable<string> GetPropertyPath(MemberExpression e)
            {
                if (e.Object is MemberExpression inner)
                {
                    foreach (var path in GetPropertyPath(inner))
                    {
                        yield return path;
                    }
                }

                if (e.Property is Identifier identifier)
                    yield return identifier.Name;
            }
        }

        public void SetBufferPoolForTestingPurposes(UnmanagedBuffersPoolWithLowMemoryHandling bufferPool)
        {
            _bufferPool = bufferPool;
        }

        public void SetAllocatorForTestingPurposes(ByteStringContext byteStringContext)
        {
            _byteStringContext = byteStringContext;
        }
    }
}
