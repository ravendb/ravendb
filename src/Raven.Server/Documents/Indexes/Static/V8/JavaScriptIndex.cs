using System;
using V8.Net;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Extensions.V8;
using Raven.Server.Documents.Indexes.Static.JavaScript.V8;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Documents.Indexes.Static
{
    public sealed partial class JavaScriptIndex
    {
        private InternalHandle GetDocumentIdV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try
            {
                var scope = CurrentIndexingScope.Current;
                scope.RegisterJavaScriptUtils(JsUtilsV8);

                return JsUtilsV8.GetDocumentId(engine, isConstructCall, self, args);
            }
            catch (Exception e) 
            {
                _lastException = e;
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private InternalHandle AttachmentsForV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try
            {
                var scope = CurrentIndexingScope.Current;
                scope.RegisterJavaScriptUtils(JsUtilsV8);

                return JsUtilsV8.AttachmentsFor(engine, isConstructCall, self, args);
            }
            catch (Exception e) 
            {
                _lastException = e;
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private InternalHandle MetadataForV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try
            {
                var scope = CurrentIndexingScope.Current;
                scope.RegisterJavaScriptUtils(JsUtilsV8);

                return JsUtilsV8.GetMetadata(engine, isConstructCall, self, args).KeepAlive();
            }
            catch (Exception e) 
            {
                _lastException = e;
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private InternalHandle TimeSeriesNamesForV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try
            {
                var scope = CurrentIndexingScope.Current;
                scope.RegisterJavaScriptUtils(JsUtilsV8);

                return JavaScriptUtilsV8.GetTimeSeriesNamesFor(engine, isConstructCall, self, args);
            }
            catch (Exception e) 
            {
                _lastException = e;
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private InternalHandle CounterNamesForV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try
            {
                var scope = CurrentIndexingScope.Current;
                scope.RegisterJavaScriptUtils(JsUtilsV8);

                return JavaScriptUtilsV8.GetCounterNamesFor(engine, isConstructCall, self, args);
            }
            catch (Exception e) 
            {
                _lastException = e;
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private InternalHandle LoadAttachmentV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try
            {
                var scope = CurrentIndexingScope.Current;
                scope.RegisterJavaScriptUtils(JsUtilsV8);

                return JavaScriptUtilsV8.LoadAttachment(engine, isConstructCall, self, args);
            }
            catch (Exception e) 
            {
                _lastException = e;
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private InternalHandle LoadAttachmentsV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try
            {
                var scope = CurrentIndexingScope.Current;
                scope.RegisterJavaScriptUtils(JsUtilsV8);

                return JavaScriptUtilsV8.LoadAttachments(engine, isConstructCall, self, args);
            }
            catch (Exception e) 
            {
                _lastException = e;
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }
    }

    public abstract partial class AbstractJavaScriptIndex : IJavaScriptContext
    {
        public V8EngineEx EngineExV8 => _scriptEngineV8Pooled.Value;
        public V8Engine EngineV8 => _scriptEngineV8Pooled.Value;
        public JavaScriptUtilsV8 JsUtilsV8;

        private PoolWithLevels<V8EngineEx>.PooledValue _scriptEngineV8Pooled;
        public V8EngineEx.ContextEx ContextExV8; 

        internal Exception _lastException;
        
        public Exception LastException
        {
            get => _lastException;
            set
            {
                _lastException = value;
            }
        }
        
        protected void InitializeV8()
        {
            var poolOfEngines = V8EngineEx.GetPool(JsOptions);
            _scriptEngineV8Pooled = poolOfEngines.GetValue();

            EngineHandle = EngineExV8;
            _engineForParsing = new JintEngineExForV8();
        }
        
        public void InitializeLockedV8()
        {
            ContextExV8 = EngineExV8.CreateAndSetContextEx(JsOptions, this);
        }
            
        public void DisposeV8()
        {
            ContextExV8.Dispose();
            _scriptEngineV8Pooled.Dispose();
        }

        protected void InitializeV82()
        {
            JsUtilsV8 = (JavaScriptUtilsV8)JsUtils;
        }
        
        private InternalHandle RecurseV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try
            {
                if (args.Length != 2)
                {
                    throw new ArgumentException("The recurse(item, func) method expects two arguments, but got: " + args.Length);
                }

                var item = args[0];
                var func = args[1];

                if (!func.IsFunction)
                    throw new ArgumentException("The second argument in recurse(item, func) must be an arrow function.");

                using (var rf = new RecursiveJsFunctionV8(EngineV8, item, func))
                    return rf.Execute();
            }
            catch (Exception e) 
            {
                _lastException = e;
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }


        //public InternalHandle jsTest;

        private InternalHandle TryConvertToNumberV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try
            {
                if (args.Length != 1)
                    throw new ArgumentException("The tryConvertToNumber(value) method expects one argument, but got: " + args.Length);

                InternalHandle value = args[0];
                InternalHandle jsRes = InternalHandle.Empty;

                if (value.IsNull || value.IsUndefined)
                    return EngineExV8.Context.ImplicitNullV8().Clone();

                if (value.IsNumberOrIntEx)
                    return jsRes.Set(value);

                if (value.IsStringEx)
                {
                    if (Double.TryParse(value.AsString, out var valueAsDbl))
                    {
                        return engine.CreateValue(valueAsDbl);
                    }
                }

                return EngineExV8.Context.ImplicitNullV8().Clone();
            }
            catch (Exception e) 
            {
                _lastException = e;
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private InternalHandle LoadDocumentV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try
            {
                if (args.Length != 2)
                {
                    throw new ArgumentException("The load(id, collection) method expects two arguments, but got: " + args.Length);
                }

                InternalHandle jsRes = InternalHandle.Empty;
                if (args[0].IsNull || args[0].IsUndefined)
                    return EngineExV8.Context.ImplicitNullV8().Clone();

                var argsMsgPrefix = "The load(id, collection) method expects the ";
                CheckIsStringV8(args[0], args[1], $"{argsMsgPrefix}first");
                CheckIsStringV8(args[1], args[0], $"{argsMsgPrefix}second");

                object doc = CurrentIndexingScope.Current.LoadDocument(null, args[0].AsString, args[1].AsString);
                if (!(doc is DynamicNullObject) && JsIndexUtils.GetValue(doc, out JsHandle jsItemHandle, keepAlive: true))
                    return jsItemHandle.V8.Item;

                return EngineExV8.Context.ImplicitNullV8().Clone();
            }
            catch (Exception e) 
            {
                _lastException = e;
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private void CheckIsStringV8(InternalHandle jsValue, InternalHandle jsValueNext, string prefix)
        {
            if (!jsValue.IsStringEx)
            {
                using (var jsStrRes = EngineHandle.JsonStringify().StaticCall(jsValue))
                using (var jsStrResNext = EngineHandle.JsonStringify().StaticCall(jsValueNext))
                    throw new ArgumentException($"{prefix} string argument, but got: {jsValue.Summary}; \nnext argument is: {jsValueNext.Summary}");
            }
        }

        private InternalHandle LoadCompareExchangeValueV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try
            {
                if (args.Length != 1)
                    throw new ArgumentException("The cmpxchg(key) method expects one argument, but got: " + args.Length);

                InternalHandle jsRes = InternalHandle.Empty;
                var keyArgument = args[0];
                if (keyArgument.IsNull || keyArgument.IsUndefined)
                    return EngineExV8.Context.ImplicitNullV8().Clone();

                if (keyArgument.IsStringEx)
                {
                    object value = CurrentIndexingScope.Current.LoadCompareExchangeValue(null, keyArgument.AsString);
                    return ConvertToJsValueV8(value);
                }
                else if (keyArgument.IsArray)
                {
                    int arrayLength =  keyArgument.ArrayLength;
                    if (arrayLength == 0)
                        return EngineExV8.Context.ImplicitNullV8().Clone();

                    var jsItems = new InternalHandle[arrayLength];
                    for (int i = 0; i < arrayLength; i++)
                    {
                        using (var key = keyArgument.GetProperty(i)) 
                        {
                            if (key.IsStringEx == false)
                                ThrowInvalidTypeV8(key, JSValueType.String);

                            object value = CurrentIndexingScope.Current.LoadCompareExchangeValue(null, key.AsString);
                            jsItems[i] = ConvertToJsValueV8(value);
                        }
                    }

                    return EngineV8.CreateArray(jsItems);
                }
                else
                {
                    throw new InvalidOperationException($"Argument '{keyArgument}' was of type '{keyArgument.ValueType}', but either string or array of strings was expected.");
                }
            }
            catch (Exception e) 
            {
                _lastException = e;
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }

            InternalHandle ConvertToJsValueV8(object value)
            {
                InternalHandle jsRes = InternalHandle.Empty;
                switch (value)
                {
                    case null:
                        return EngineExV8.Context.ImplicitNullV8().Clone();

                    case DynamicNullObject dno:
                    {
                        var dynamicNull = dno.IsExplicitNull ? EngineExV8.Context.ExplicitNullV8().Clone() : EngineExV8.Context.ImplicitNullV8().Clone();
                        return dynamicNull;
                    }

                    case DynamicBlittableJson dbj:
                    {
                        BlittableObjectInstanceV8 boi = new BlittableObjectInstanceV8(JsUtilsV8, null, dbj.BlittableJson, id: null, lastModified: null, changeVector: null);
                        return boi.CreateObjectBinder(true);
                    }

                    default:
                        return JsUtilsV8.TranslateToJs(context: null, value, true);
                }
            }

            static void ThrowInvalidTypeV8(InternalHandle jsValue, JSValueType expectedType)
            {
                throw new InvalidOperationException($"Argument '{jsValue}' was of type '{jsValue.ValueType}', but '{expectedType}' was expected.");
            }
        }
    }
}
