using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
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

namespace Raven.Server.Documents.Indexes.Static
{
    public class JavaScriptReduceOperation
    {
        public JavaScriptReduceOperation(ScriptFunctionInstance reduce, ScriptFunctionInstance key, Engine engine, JintPreventResolvingTasksReferenceResolver resolver)
        {
            Reduce = reduce ?? throw new ArgumentNullException(nameof(reduce));
            Key = key ?? throw new ArgumentNullException(nameof(key));
            Engine = engine;
            _resolver = resolver;
            GetReduceFieldsNames();

            _groupedItems = null;
        }

        private readonly JsValue[] _oneItemArray = new JsValue[1];

        private readonly JintPreventResolvingTasksReferenceResolver _resolver;

        private Dictionary<BlittableJsonReaderObject, List<BlittableJsonReaderObject>> _groupedItems;

        private struct GroupByKeyComparer : IEqualityComparer<BlittableJsonReaderObject>
        {
            private readonly JavaScriptReduceOperation _parent;
            private readonly ReduceKeyProcessor _xKey;
            private readonly ReduceKeyProcessor _yKey;
            private BlittableJsonReaderObject _lastUsedBlittable;
            private BlittableJsonReaderObject _lastUsedBucket;
            private readonly ByteStringContext _allocator;

            public GroupByKeyComparer(JavaScriptReduceOperation parent, UnmanagedBuffersPoolWithLowMemoryHandling buffersPool, ByteStringContext allocator)
            {
                _parent = parent;
                _allocator = allocator;
                _xKey = new ReduceKeyProcessor(_parent._groupByFields.Length, buffersPool);
                _yKey = new ReduceKeyProcessor(_parent._groupByFields.Length, buffersPool);
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
                foreach (var item in _groupedItems.Values)
                {
                    Engine.ResetCallStack();
                    Engine.ResetConstraints();

                    _oneItemArray[0] = ConstructGrouping(item);
                    JsValue jsItem;
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

        private JsValue ConstructGrouping(List<BlittableJsonReaderObject> values)
        {
            var jsValues = ConstructValues();
            var jsKey = ConstructKey();

            var result = new ObjectInstance(Engine);

            result.Set("values", jsValues, false);
            result.Set("key", jsKey, false);

            return result;

            JsValue ConstructKey()
            {
                if (_singleField)
                {
                    var index = values[0].GetPropertyIndex(_groupByFields[0].Name);
                    if (index != -1)
                    {
                        BlittableJsonReaderObject.PropertyDetails prop = default;
                        values[0].GetPropertyByIndex(index, ref prop);

                        return JsValue.FromObject(Engine, prop.Value);
                    }

                    return JsValue.Null;
                }

                var key = new ObjectInstance(Engine);

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

                        JsValue jsValue = value switch
                        {
                            BlittableJsonReaderObject bjro => new BlittableObjectInstance(Engine, null, bjro, null, null, null),
                            Document doc => new BlittableObjectInstance(Engine, null, doc.Data, doc),
                            LazyStringValue lsv => new JsString(lsv.ToString()),
                            LazyCompressedStringValue lcsv => new JsString(lcsv.ToString()),
                            LazyNumberValue lnv => new JsNumber(lnv.ToDouble(CultureInfo.InvariantCulture)),
                            _ => JsValue.FromObject(Engine, value)
                        };

                        key.Set(propertyName, jsValue, throwOnError: false);
                    }
                }

                return key;
            }

            ArrayInstance ConstructValues()
            {
                var items = new PropertyDescriptor[values.Count];
                for (var i = 0; i < values.Count; i++)
                {
                    var val = values[i];

                    if (JavaScriptIndexUtils.GetValue(Engine, val, out var jsValue, isMapReduce: true) == false)
                        continue;

                    items[i] = new PropertyDescriptor(jsValue, true, true, true);
                }

                var jsArray = new ArrayInstance(Engine, items);
                jsArray.SetPrototypeOf(Engine.Array.PrototypeObject);
                jsArray.PreventExtensions();

                return jsArray;
            }
        }

        public Engine Engine { get; }

        public ScriptFunctionInstance Reduce { get; }
        public ScriptFunctionInstance Key { get; }
        public string ReduceString { get; internal set; }

        private CompiledIndexField[] _groupByFields;
        private bool _singleField;
        private UnmanagedBuffersPoolWithLowMemoryHandling _bufferPool;
        private ByteStringContext _byteStringContext;

        internal CompiledIndexField[] GetReduceFieldsNames()
        {
            if (_groupByFields != null)
                return _groupByFields;

            var ast = Key.FunctionDeclaration;
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

                        var propertyName = property.GetKey(Engine);
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
