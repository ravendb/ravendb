using System;
using System.Collections.Generic;
using System.Threading;
using Jint;
using Jint.Runtime.Interop;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.Config;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using V8.Net;

namespace Raven.Server.Documents.Patch.Jint
{
    public class SingleRunJint : SingleRun<JsHandleJint>
    {
        public Engine ScriptEngineJint;

        private JintPreventResolvingTasksReferenceResolver _refResolverJint = null;

        public SingleRunJint(DocumentDatabase database, RavenConfiguration configuration, ScriptRunnerJint runner, List<string> scriptsSource,
            bool executeScriptsSource = true)
            : base(database, configuration, runner, scriptsSource)
        {
            _refResolverJint = new JintPreventResolvingTasksReferenceResolver();
            var engine = new JintEngineEx(configuration, _refResolverJint);
            ScriptEngineJint = engine.Engine;
            ScriptEngineHandle = engine;
            JsUtils = new JavaScriptUtilsJint(runner, engine);
            JsBlittableBridge = new JsBlittableBridgeJint(engine);
            Initialize(executeScriptsSource);
        }

        public override void SetContext()
        {
          //noop
        }

        protected override JsHandleJint CreateObjectBinder(object value)
        {
            var obj = new ObjectWrapper(ScriptEngineJint, value);
            return new JsHandleJint(obj);
        }

        protected override bool TryGetValueFromBoi(IBlittableObjectInstance iboi, string propName, out IBlittableObjectProperty<JsHandleJint> blittableObjectProperty, out bool b)
        {
            var boi = (BlittableObjectInstanceJint)iboi;
            if (boi.TryGetValue(propName, out blittableObjectProperty, out b))
            {
                return true;
            }

            blittableObjectProperty = default;
            b = false;
            return false;
        }

        protected override string GetTypes(JsHandleJint value) => $"JintType({value.ValueType}) .NETType({value.GetType().Name})";

        protected override void SetArgs(JsHandleJint[] args, IBlittableObjectInstance boi)
        {
            //TODO: egor check this overload
            _refResolverJint.ExplodeArgsOn(args[0].Item, (BlittableObjectInstanceJint)boi);
        }

        public override void SetArgsInternal()
        {
            if (_args.Length > 1 && _args[1].Object is BlittableObjectInstanceJint boi)
                _refResolverJint.ExplodeArgsOn(null, boi);
        }

        public override ScriptRunnerResult<JsHandleJint> Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, string method, string documentId, object[] args, QueryTimingsScope scope = null,
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


              

                            //  return new ScriptRunnerResult(this, jsRes);
                            return new ScriptRunnerResultJint(this, jsRes);
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

        protected override void DisposeArgs()
        {
            _refResolverJint.ExplodeArgsOn(null, null);
        }
    }
}
