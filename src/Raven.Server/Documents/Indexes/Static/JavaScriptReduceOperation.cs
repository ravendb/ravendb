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
using V8.Net;
using Raven.Server.Extensions;
using Raven.Server.Documents.Indexes.Static.Utils;

namespace Raven.Server.Documents.Indexes.Static
{
    public class JavaScriptReduceOperation
    {
        public JavaScriptReduceOperation(ScriptFunctionInstance reduce, ScriptFunctionInstance key, Engine engine,
            InternalHandle reduceV8, InternalHandle keyV8, JavaScriptIndexUtils javaScriptIndexUtilsV8)
        {
            Reduce = reduce ?? throw new ArgumentNullException(nameof(reduce));
            Key = key ?? throw new ArgumentNullException(nameof(key));
            Engine = engine;
            GetReduceFieldsNames();

            _groupedItems = null;

            if (reduceV8.IsUndefined || reduceV8.IsNull)
                throw new ArgumentNullException(nameof(reduceV8));
            ReduceV8 = new InternalHandle(reduceV8, true);

            if (keyV8.IsUndefined || keyV8.IsNull)
                throw new ArgumentNullException(nameof(keyV8));
            KeyV8 = new InternalHandle(keyV8, true);

            JavaScriptIndexUtilsV8 = javaScriptIndexUtilsV8;
            JavaScriptUtilsV8 = JavaScriptIndexUtilsV8.JavaScriptUtils;
            EngineV8 = JavaScriptIndexUtilsV8.Engine;

        }

        ~JavaScriptReduceOperation()
        {
            ReduceV8.Dispose();
            KeyV8.Dispose();
        }


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

        public IEnumerable<InternalHandle> IndexingFunction(IEnumerable<dynamic> items)
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
                    EngineV8.ResetCallStack();
                    EngineV8.ResetConstraints();

                    InternalHandle jsRes = InternalHandle.Empty;
                    try
                    {
                        using (var jsGrouping = ConstructGrouping(item))
                        {
                            bool res = false;
                            jsRes = ReduceV8.StaticCall(jsGrouping);
                            jsRes.ThrowOnError();
                            if (jsRes.IsObject == false)
                                throw new JavaScriptIndexFuncException($"Failed to execute {ReduceString}", new Exception($"Reduce result is not object: {jsRes.ToString()}"));
                        }
                    }
                    catch (V8Exception jse)
                    {
                        var (message, success) = JavaScriptIndexFuncException.PrepareErrorMessageForJavaScriptIndexFuncException(ReduceString, jse);
                        if (success == false)
                            throw new JavaScriptIndexFuncException($"Failed to execute {ReduceString}", jse);
                        throw new JavaScriptIndexFuncException($"Failed to execute reduce script, {message}", jse);
                    }
                    catch (Exception e)
                    {
                        jsRes.Dispose();
                        throw new JavaScriptIndexFuncException($"Failed to execute {ReduceString}", e);
                    }
                    yield return jsRes;
                }
            }
            finally
            {
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

        private InternalHandle ConstructGrouping(List<BlittableJsonReaderObject> values)
        {
            var result = EngineV8.CreateObject();
            result.SetProperty("values", ConstructValues());
            result.SetProperty("key", ConstructKey());

            return result;

            InternalHandle ConstructKey()
            {
                if (_singleField)
                {
                    var index = values[0].GetPropertyIndex(_groupByFields[0].Name);
                    if (index != -1)
                    {
                        BlittableJsonReaderObject.PropertyDetails prop = default;
                        values[0].GetPropertyByIndex(index, ref prop);

                        return EngineV8.FromObject(prop.Value);
                    }

                    return EngineV8.CreateNullValue();
                }

                InternalHandle jsRes = InternalHandle.Empty;
                jsRes = EngineV8.CreateObject();
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
                            BlittableJsonReaderObject bjro => ((Func<BlittableJsonReaderObject, InternalHandle>)((BlittableJsonReaderObject bjro) => {
                                var boi = new BlittableObjectInstance(JavaScriptUtilsV8, null, bjro, null, null, null);
                                return boi.CreateObjectBinder(); // maybe better move to FromObject?
                            }))(bjro),
                            Document doc => ((Func<Document, InternalHandle>)((Document doc) => {
                                var boi = new BlittableObjectInstance(JavaScriptUtilsV8, null, doc.Data, doc);
                                return boi.CreateObjectBinder(); // maybe better  move to FromObject?
                            }))(doc),
                            LazyNumberValue lnv => EngineV8.CreateValue(lnv.ToDouble(CultureInfo.InvariantCulture)), // maybe better  move to FromObject?
                            _ =>  EngineV8.FromObject(value)
                        };

                        jsRes.SetProperty(propertyName, jsValue);
                    }
                }

                return jsRes;
            }

            InternalHandle ConstructValues()
            {
                int arrayLength =  values.Count;
                var jsItems = new InternalHandle[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    var val = values[i];

                    if (JavaScriptIndexUtilsV8.GetValue(val, out InternalHandle jsValue, isMapReduce: true) == false)
                        continue;

                    jsItems[i] = jsValue;
                }

                return EngineV8.CreateArrayWithDisposal(jsItems);
            }
        }

        public Engine Engine { get; }

        public ScriptFunctionInstance Reduce { get; } // is not used actually
        public ScriptFunctionInstance Key { get; }
        public string ReduceString { get; internal set; }        


        public JavaScriptIndexUtils JavaScriptIndexUtilsV8 { get; }
        public JavaScriptUtils JavaScriptUtilsV8 { get; }
        public V8EngineEx EngineV8 { get; }

        public InternalHandle ReduceV8 { get; }
        public InternalHandle KeyV8 { get; }

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
