using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.Config;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using V8.Net;

namespace Raven.Server.Documents.Patch.V8
{
    public class SingleRunV8 : SingleRun<JsHandleV8>, IJavaScriptContext
    {
        private PoolWithLevels<V8EngineEx>.PooledValue _scriptEngineV8Pooled;
        public V8Engine ScriptEngineV8;
        private V8EngineEx.ContextEx _contextExV8;
        public V8EngineEx ScriptEngineExV8;

        private Exception _lastException;

        public Exception LastException
        {
            get => _lastException;
            set
            {
                _lastException = value;
            }
        }

        public SingleRunV8(DocumentDatabase database, RavenConfiguration configuration, ScriptRunnerV8 runner, List<string> scriptsSource, bool executeScriptsSource = true)
            : base(database, configuration, runner, scriptsSource)
        {
            _scriptEngineV8Pooled = V8EngineEx.GetPool(configuration).GetValue();
            var engine = _scriptEngineV8Pooled.Value;
            //TODO: egor this is passed to only set "last exception"
            _contextExV8 = engine.CreateAndSetContextEx(configuration, jsContext: this);
            //TODO: egor this is set in line above??? not needed here?
            engine.Context = _contextExV8;
            ScriptEngineExV8 = engine;
            ScriptEngineV8 = engine.Engine;
            ScriptEngineHandle = engine;

            JsUtils = new JavaScriptUtilsV8(runner, engine);
            JsBlittableBridge = new JsBlittableBridgeV8(engine);
            Initialize(executeScriptsSource);
        }

        //TODO: egor what to do with the finalizer ? why not IDisposable ##################################
        //TODO: should move to the try-finally of Run() ?
        ~SingleRunV8()
        {
            DisposeArgs();
            DisposeV8();
        }


        private void DisposeV8()
        {
            _contextExV8.Dispose();
            _scriptEngineV8Pooled.Dispose();
        }

        public override void SetContext()
        {
            ScriptEngineExV8.Context = _contextExV8;
        }

        protected override JsHandleV8 CreateObjectBinder(object value)
        {
            InternalHandle internalHandle;
            switch (value)
            {
                case LazyNumberValue lnv:
                    internalHandle = ScriptEngineV8.CreateObjectBinder(lnv);
                    break;
                case LazyStringValue lsv:
                    internalHandle = ScriptEngineV8.CreateObjectBinder(lsv);
                    break;
                case LazyCompressedStringValue lcsv:
                    internalHandle = ScriptEngineV8.CreateObjectBinder(lcsv);
                    break;
                default:
                    throw new Exception(nameof(value));
            }

            return new JsHandleV8(ref internalHandle);
        }

        protected override bool TryGetValueFromBoi(IBlittableObjectInstance iboi, string propName, out IBlittableObjectProperty<JsHandleV8> blittableObjectProperty, out bool b)
        {
            var boi = (BlittableObjectInstanceV8)iboi;
            if (boi.TryGetValue(propName, out blittableObjectProperty, out b))
            {
                return true;
            }

            blittableObjectProperty = default;
            b = false;
            return false;
        }

        protected override string GetTypes(JsHandleV8 value)
        {
            return $"V8Type({value.ValueType}) .NETType({value.GetType().Name})";
        }

        public override void SetArgsInternal()
        {
            // this set the private _args
            if (_args.Length > 1 && _args[1].Object is BlittableObjectInstanceV8 boi)
            {
                var global = ScriptEngineV8.GlobalObject;
                foreach (var propertyNameOrig in boi.EnumerateOwnPropertiesUnordered())
                {
                    var desc = boi.GetOwnProperty(propertyNameOrig);
                    if (desc != null)
                    {
                        var valueNew = desc.Value;
                        var propertyName = "$" + propertyNameOrig;
                        if (global.HasProperty(propertyName))
                        {
                            using (var valuePrev = global.GetProperty(propertyName))
                            {
                                if (ReferenceEquals(valuePrev.Object, valueNew.Object))
                                {
                                    return; // ExplodeArgsOn can be called after SetArgs in ScriptRunner, in this case we can just skip repeated setting
                                }
                                else
                                {
                                    var valueNewStr = ScriptEngineHandle.JsonStringify().Item.StaticCall(valueNew);
                                    var valuePrevStr = ScriptEngineHandle.JsonStringify().Item.StaticCall(valuePrev);
                                    throw new ArgumentException(
                                        $"Can't set argument '{propertyName}' as property on global object as it already exists with value {valuePrevStr}, new value {valueNewStr}");
                                }
                            }
                        }

                        if (global.SetProperty(propertyName, valueNew.Clone()) == false)
                        {
                            throw new JavaScriptException($"Failed to set property {propertyName} on global object");
                        }
                    }
                }
            }
        }

        public override ScriptRunnerResult<JsHandleV8> Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, string method, string documentId, object[] args, QueryTimingsScope scope = null,
            CancellationToken token = default)
        {
            lock (ScriptEngineHandle)
            {
                //     _lastException = null;

                _docsCtx = docCtx;
                _jsonCtx = jsonCtx ?? ThrowArgumentNull();
                _scope = scope;
                _token = token;

                JsUtils.Reset(_jsonCtx);

                Reset();
                OriginalDocumentId = documentId;

                bool isMemorySnapshotMade = false;

                try
                {
                    if (ScriptEngineHandle.IsMemoryChecksOn)
                    {
                        ScriptEngineHandle.MakeSnapshot("single_run");
                    }

                    SetArgs(jsonCtx, method, args);

                    using (var jsMethod = ScriptEngineHandle.GetGlobalProperty(method))
                    {
                        if (jsMethod.IsUndefined)
                            throw new InvalidOperationException(
                                $"Failed to get global function '{method}', global object is: {ScriptEngineHandle.JsonStringify().StaticCall(ScriptEngineHandle.GlobalObject)}");

                        if (!jsMethod.IsFunction)
                            throw new InvalidOperationException(
                                $"Obtained {method} global property is not a function: {ScriptEngineHandle.JsonStringify().StaticCall(method)}");

#if false //DEBUG
                            var argsStr = "";
                            for (int i = 0; i < _args.Length; i++)
                            {
                                using (var jsArgStr = ScriptEngineHandle.JsonStringify().StaticCall(_args[i]))
                                {
                                    var argStr = jsArgStr.IsUndefined ? "undefined" : jsArgStr.AsString;
                                    argsStr += argStr + "\n\n";
                                }
                            }
#endif

                        using (var jsRes = jsMethod.StaticCall(_args))
                        {
                            if (jsRes.IsError)
                            {
                                //if (_lastException != null)
                                //{
                                //    ExceptionDispatchInfo.Capture(_lastException).Throw();
                                //}
                                //else
                                //{
                                //    jsRes.ThrowOnError();
                                //}
                            }
#if false //DEBUG
                                var resStr = "";
                                using (var jsResStr = ScriptEngineHandle.JsonStringify().StaticCall(jsRes))
                                {
                                    resStr = jsResStr.IsUndefined ? "undefined" : jsResStr.AsString;
                                }
#endif

                                //  ScriptEngineV8.AddToLastMemorySnapshotBefore(jsRes.V8.Item);
                                isMemorySnapshotMade = true;

                            //  return new ScriptRunnerResult(this, jsRes);
                            return new ScriptRunnerResultV8(this, jsRes);
                        }
                    }
                }
                catch (JavaScriptException e)
                {
                    //ScriptRunnerResult is in charge of disposing of the disposable but it is not created (the clones did)
                    //JsUtilsJint.Clear();
                    //throw CreateFullError(e);
                    throw new Exception();
                }
                catch (V8Exception e)
                {
                    //ScriptRunnerResult is in charge of disposing of the disposable but it is not created (the clones did)
                    //JsUtilsV8.Clear();
                    //throw CreateFullError(e);
                    throw new Exception();
                }
                catch (Exception)
                {
                    JsUtils.Clear();
                    throw;
                }
                finally
                {
                    DisposeArgs();
                    _scope = null;
                    _loadScope = null;
                    _docsCtx = null;
                    _jsonCtx = null;
                    _token = default;
                    //   _lastException = null;

                    ScriptEngineHandle.ForceGarbageCollection();
                    if (ScriptEngineHandle.IsMemoryChecksOn && isMemorySnapshotMade)
                    {
                        ScriptEngineHandle.CheckForMemoryLeaks("single_run");
                    }
                }
            }
        }

        protected override void SetArgs(JsHandleV8[] args, IBlittableObjectInstance boi)
        {
            //TODO: egor check this in original implementation it was setting the _args 
            var global = ScriptEngineV8.GlobalObject;
            var boi2 = (BlittableObjectInstanceV8)boi;
            foreach (var propertyNameOrig in boi2.EnumerateOwnPropertiesUnordered())
            {
                var desc = boi2.GetOwnProperty(propertyNameOrig);
                if (desc != null)
                {
                    var valueNew = desc.Value;
                    var propertyName = "$" + propertyNameOrig;
                    if (global.HasProperty(propertyName))
                    {
                        using (var valuePrev = global.GetProperty(propertyName))
                        {
                            if (ReferenceEquals(valuePrev.Object, valueNew.Object))
                            {
                                return; // ExplodeArgsOn can be called after SetArgs in ScriptRunner, in this case we can just skip repeated setting
                            }
                            else
                            {
                                var valueNewStr = ScriptEngineHandle.JsonStringify().Item.StaticCall(valueNew);
                                var valuePrevStr = ScriptEngineHandle.JsonStringify().Item.StaticCall(valuePrev);
                                throw new ArgumentException(
                                    $"Can't set argument '{propertyName}' as property on global object as it already exists with value {valuePrevStr}, new value {valueNewStr}");
                            }
                        }
                    }

                    if (!global.SetProperty(propertyName, valueNew.Clone()))
                    {
                        throw new JavaScriptException($"Failed to set property {propertyName} on global object");
                    }
                }
            }
        }

        protected override void DisposeArgs()
        {
            if (_args.Length == 0)
                return;

            if (_args.Length > 1 && _args[1].Object is BlittableObjectInstanceV8 boi)
            {
                var global = ScriptEngineV8.GlobalObject;
                foreach (var propertyNameOrig in boi.EnumerateOwnPropertiesUnordered())
                {
                    var propertyName = "$" + propertyNameOrig;
                    if (global.HasProperty(propertyName))
                    {
                        if (!global.DeleteProperty(propertyName))
                        {
                            throw new JavaScriptException($"Failed to delete property {propertyName} on global object");
                        }
                    }
                }
            }

            for (int i = 0; i < _args.Length; ++i)
            {
                _args[i].Dispose();
                _args[i] = default;
            }
            //TODO: egor just allocate new empty array?
        //    Array.Clear(_args, 0, _args.Length);
        }

        //TODO: egor end ###################################################
        }
}
