using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Jint;
using Jint.Native;
using Jint.Native.Function;
using Raven.Server.Documents.Patch;
using Raven.Server.Extensions;
using Esprima.Ast;
using Jint.Native.Array;
using Jint.Runtime;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Sparrow.Json;
using Sparrow;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.ServerWide;
using System.Runtime.CompilerServices;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes.Static
{
    public class JavaScriptReduceOperation
    {
        public JavaScriptReduceOperation(ScriptFunctionInstance reduce, ScriptFunctionInstance key, Engine engine, JintPreventResolvingTasksReferenceResolver resolver)
        {
            Reduce = reduce;
            Key = key;
            Engine = engine;
            _resolver = resolver;
            GetReduceFieldsNames();

            _groupedItems = null;
        }

        private readonly JsValue[] _oneItemArray = new JsValue[1];

        private JintPreventResolvingTasksReferenceResolver _resolver;

        private Dictionary<BlittableJsonReaderObject, List<BlittableJsonReaderObject>> _groupedItems;

        private struct GroupByKeyComparer : IEqualityComparer<BlittableJsonReaderObject>
        {
            JavaScriptReduceOperation _parent;
            ReduceKeyProcessor _xKey, _yKey;
            private BlittableJsonReaderObject _lastUsedBlittable;
            private BlittableJsonReaderObject _lastUsedBucket;
            private ByteStringContext _allocator;

            public GroupByKeyComparer(JavaScriptReduceOperation parent, UnmanagedBuffersPoolWithLowMemoryHandling buffersPool, ByteStringContext allocator)
            {
                _parent = parent;
                _allocator = allocator;
                _xKey = new ReduceKeyProcessor(_parent._groupByFields.Count(), buffersPool);
                _yKey = new ReduceKeyProcessor(_parent._groupByFields.Count(), buffersPool);
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
                    object xVal = null;
                    if (xCalculated == false)
                        xHasField = x.TryGet(field.Name, out xVal);
                    object yVal = null;
                    if (yCalculated == false && xCalculated == false)
                    {
                        var yHasField = y.TryGet(field.Name, out yVal);
                        if (xHasField != yHasField)
                            return false;
                    }

                    if (xCalculated == false)
                        _xKey.Process(_allocator, xVal);

                    if (yCalculated == false)
                        _yKey.Process(_allocator, yVal);
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
                    var xHasField = obj.TryGet(field.Name, out object xVal);
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
                foreach (var item in _groupedItems.Values)
                {
                    Engine.ResetCallStack();
                    Engine.ResetStatementsCount();
                    Engine.ResetTimeoutTicks();

                    _oneItemArray[0] = ConstructValues(item);
                    JsValue jsItem = null;
                    try
                    {
                        jsItem = Reduce.Call(JsValue.Null, _oneItemArray).AsObject();
                    }
                    catch (JavaScriptException jse)
                    {
                        var (message, success) = JavaScriptIndexFuncException.PrepareErrorMessageForJavaScriptIndexFuncException(ReduceString, jse);
                        if (success == false)
                            throw new JavaScriptIndexFuncException($"Failed to execute {ReduceString}", jse);
                        throw new JavaScriptIndexFuncException($"Failed to execute reduce script, {message}", jse);
                    }
                    catch (Exception e)
                    {
                        throw new JavaScriptIndexFuncException($"Failed to execute {ReduceString}", e);
                    }
                    yield return jsItem;
                    _resolver.ExplodeArgsOn(null, null);
                }
            }
            finally
            {
                _oneItemArray[0] = null;
                _groupedItems.Clear();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureGroupItemCreated()
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

                _groupedItems = new Dictionary<BlittableJsonReaderObject, List<BlittableJsonReaderObject>>(new GroupByKeyComparer(this, _bufferPool, _byteStringContext));
            }
        }

        private JsValue ConstructValues(List<BlittableJsonReaderObject> values)
        {
            var items = new PropertyDescriptor[values.Count];
            for (int j = 0; j < values.Count; j++)
            {
                var val = values[j];

                if (JavaScriptIndexUtils.GetValue(Engine, val, out JsValue jsValue, true) == false)
                    continue;
                items[j] = new PropertyDescriptor(jsValue, true, true, true);
            }
            var jsArray = new ArrayInstance(Engine, items);
            jsArray.Prototype = Engine.Array.PrototypeObject;
            jsArray.Extensible = false;

            var result = new ObjectInstance(Engine)
            {
                Extensible = true
            };
            result.Put("values", jsArray, false);
            if (_singleField)
            {
                var index = values[0].GetPropertyIndex(_groupByFields[0].Name);
                if (index != -1)
                {
                    BlittableJsonReaderObject.PropertyDetails prop = default;
                    values[0].GetPropertyByIndex(index, ref prop, addObjectToCache: false);
                    result.Put("key", JsValue.FromObject(Engine, prop.Value), false);
                }
            }
            else
            {
                var key = new ObjectInstance(Engine)
                {
                    Extensible = true
                };
                result.Put("key", key, false);
                for (int i = 0; i < _groupByFields.Length; i++)
                {
                    var index = values[0].GetPropertyIndex(_groupByFields[i].Name);
                    if (index != -1)
                    {
                        BlittableJsonReaderObject.PropertyDetails prop = default;
                        values[0].GetPropertyByIndex(index, ref prop, addObjectToCache: false);
                        key.Put(_groupByFields[i].Name, JsValue.FromObject(Engine, prop.Value), false);
                    }
                }
            }
            return result;
        }

        public Engine Engine { get; }

        public ScriptFunctionInstance Reduce { get; }
        public ScriptFunctionInstance Key { get; }
        public string ReduceString { get; internal set; }

        private Field[] _groupByFields;
        private bool _singleField;
        private UnmanagedBuffersPoolWithLowMemoryHandling _bufferPool;
        private ByteStringContext _byteStringContext;

        internal Field[] GetReduceFieldsNames()
        {
            if (_groupByFields != null)
                return _groupByFields;

            var ast = Key.GetFunctionAst();
            var body = ast.Body.Body;

            if (body.Count != 1)
            {
                throw new InvalidOperationException($"Was requested to get reduce fields from a scripted function in an unexpected format, expected a single return statement got {body.Count}.");
            }

            var @params = ast.Params;
            if (@params.Count != 1)
            {
                throw new InvalidOperationException($"Was requested to get reduce fields from a scripted function in an unexpected format, expected a single argument but got {@params.Count}.");
            }

            if (@params[0] is Identifier == false)
            {
                throw new InvalidOperationException($"Was requested to get reduce fields from a scripted function in an unexpected format, expected a single argument of type 'Identifier' but got {@params[0].GetType().Name}.");
            }

            var actualBody = body[0];
            if (!(actualBody is ReturnStatement returnStatement))
            {
                throw new InvalidOperationException($"Was requested to get reduce fields from a scripted function in an unexpected format, expected a single return statement got a statement of type {actualBody.GetType().Name}.");
            }

            if (!(returnStatement.Argument is ObjectExpression oe))
            {
                if (returnStatement.Argument is StaticMemberExpression sme && sme.Property is Identifier id)
                {
                    _groupByFields = new[] { new SimpleField(id.Name) };
                    _singleField = true;

                    return _groupByFields;
                }
                throw new InvalidOperationException($"Was requested to get reduce fields from a scripted function in an unexpected format, expected a single return object expression statement got a statement of type {actualBody.GetType().Name}.");
            }

            var cur = new HashSet<Field>();
            foreach (var prop in oe.Properties)
            {
                var fieldName = prop.Key.GetKey();
                cur.Add(new SimpleField(fieldName));
            }
            _groupByFields = cur.ToArray();

            return _groupByFields;
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
