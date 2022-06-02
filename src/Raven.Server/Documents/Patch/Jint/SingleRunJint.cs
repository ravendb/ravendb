using System;
using System.Collections.Generic;
using System.Threading;
using Jint;
using Jint.Runtime;
using Jint.Runtime.Interop;
using Raven.Server.Config;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

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

        protected override JsHandleJint CreateObjectBinder(BlittableJsonToken type, object value)
        {
            switch (type)
            {
                case BlittableJsonToken.Integer:
                case BlittableJsonToken.LazyNumber:
                case BlittableJsonToken.String:
                case BlittableJsonToken.CompressedString:
                    ObjectWrapper obj = new ObjectWrapper(ScriptEngineJint, value);
                    return new JsHandleJint(obj);
                default:
                    throw new InvalidOperationException("scalarToRawString(document, lambdaToField) lambda to field must return either raw numeric or raw string types");
            }
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
                    JsUtils.Clear();
                    throw CreateFullError(e);
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

        protected override Client.Exceptions.Documents.Patching.JavaScriptException CreateFullError(Exception ex)
        {
            if (ex is JavaScriptException e)
            {
                string msg;
                if (e.Error.IsString())
                    msg = e.Error.AsString();
                else if (e.Error.IsObject())
                    msg = JsBlittableBridge.Translate(_jsonCtx, new JsHandleJint(e.Error.AsObject())).ToString();
                else
                    msg = e.Error.ToString();

                msg = "At " + e.Column + ":" + e.LineNumber + " " + msg;
                var javaScriptException = new Client.Exceptions.Documents.Patching.JavaScriptException(msg, e);
                return javaScriptException;
            }

            throw new ArgumentException($"expected JavaScriptException but got {ex.GetType().FullName}");
        }
        protected override void DisposeArgs()
        {
            _refResolverJint.ExplodeArgsOn(null, null);
        }
    }
}
