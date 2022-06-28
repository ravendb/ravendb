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
        private V8Engine ScriptEngineV8;
      private V8EngineEx ScriptEngineExV8;
        public Exception LastException { get; set; }
        public SingleRunV8(DocumentDatabase database, RavenConfiguration configuration, ScriptRunnerV8 runner, List<string> scriptsSource, bool executeScriptsSource = true)
            : base(database, configuration, runner, scriptsSource)
        {
            var engine = V8EngineEx.GetEngine(configuration, jsContext: this, database.DatabaseShutdown);
            ScriptEngineExV8 = engine;
            ScriptEngineV8 = engine.Engine;
            EngineHandle = engine;

            JsUtils = new JavaScriptUtilsV8(runner, engine);
            JsBlittableBridge = new JsBlittableBridgeV8(engine);
            Initialize(executeScriptsSource);
        }

        protected override JsHandleV8 CreateErrorAndSetLastExceptionIfNeeded(Exception e, JSValueType errorType)
        {
            LastException = e;
            return EngineHandle.CreateError(e, errorType);
        }

        public override void CleanInternal()
        {
            V8EngineEx.ReturnEngine(ScriptEngineExV8);
        }

        protected override JsHandleV8 CreateObjectBinder(BlittableJsonToken type, object value)
        {
            InternalHandle internalHandle;
            switch (type)
            {
                case BlittableJsonToken.Integer:
                    return EngineHandle.CreateValue((long)value);
                case BlittableJsonToken.LazyNumber:
                    internalHandle = ScriptEngineV8.CreateObjectBinder((LazyNumberValue)value);
                    break;
                case BlittableJsonToken.String:
                    internalHandle = ScriptEngineV8.CreateObjectBinder((LazyStringValue)value);
                    break;
                case BlittableJsonToken.CompressedString:
                    internalHandle = ScriptEngineV8.CreateObjectBinder((LazyCompressedStringValue)value);
                    break;

                default:
                    throw new InvalidOperationException("scalarToRawString(document, lambdaToField) lambda to field must return either raw numeric or raw string types");
            }

            return new JsHandleV8(ref internalHandle);
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
                                    var valueNewStr = EngineHandle.JsonStringify().Item.StaticCall(valueNew);
                                    var valuePrevStr = EngineHandle.JsonStringify().Item.StaticCall(valuePrev);
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
                if (EngineHandle.IsMemoryChecksOn)
                {
                    EngineHandle.MakeSnapshot("single_run");
                }

                SetArgs(jsonCtx, method, args);

                using (var jsMethod = EngineHandle.GetGlobalProperty(method))
                {
                    if (jsMethod.IsUndefined)
                        throw new InvalidOperationException(
                            $"Failed to get global function '{method}', global object is: {EngineHandle.JsonStringify().StaticCall(EngineHandle.GlobalObject)}");

                    if (!jsMethod.IsFunction)
                        throw new InvalidOperationException(
                            $"Obtained {method} global property is not a function: {EngineHandle.JsonStringify().StaticCall(method)}");

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
                            jsRes.ThrowOnError();
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

                        return new ScriptRunnerResultV8(this, jsRes);
                    }
                }
            }
            catch (V8Exception e)
            {
                //ScriptRunnerResult is in charge of disposing of the disposable but it is not created (the clones did)
                JsUtils.Clear();
                CreateFullError(e);
                //TODO: egor never hit?
                throw;
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
                LastException = null;

                EngineHandle.ForceGarbageCollection();
                if (EngineHandle.IsMemoryChecksOn && isMemorySnapshotMade)
                {
                    EngineHandle.CheckForMemoryLeaks("single_run");
                }
            }
        }

        protected override JavaScriptException CreateFullError(Exception e)
        {
            if (LastException != null)
                throw LastException;

            throw new JavaScriptException(e.Message, e);
        }

        protected override void SetArgs(JsHandleV8[] args, IBlittableObjectInstance<JsHandleV8> boi)
        {
            //TODO: egor check this in original implementation it was setting the _args 
            var global = ScriptEngineV8.GlobalObject;
            var boi2 = (BlittableObjectInstanceV8)boi;

            //TODO: egor cehck which to use
            foreach (var propertyNameOrig in boi2.EnumerateOwnPropertiesUnordered())
          //  foreach (var propertyNameOrig in boi2.EnumerateOwnProperties())
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
                                var valueNewStr = EngineHandle.JsonStringify().Item.StaticCall(valueNew);
                                var valuePrevStr = EngineHandle.JsonStringify().Item.StaticCall(valuePrev);
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
            Array.Clear(_args, 0, _args.Length);
        }

        //TODO: egor end ###################################################
        }
}
