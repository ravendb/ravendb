using System;
using System.Collections.Generic;
using System.Threading;
using Jint;
using Jint.Native.Function;
using Jint.Runtime;
using Jint.Runtime.Interop;
using Raven.Server.Config;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Extensions.Jint;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using V8.Net;

namespace Raven.Server.Documents.Patch.Jint
{
    public class SingleRunJint : SingleRun<JsHandleJint>
    {
        private Engine _engine;

        private JintPreventResolvingTasksReferenceResolver _refResolverJint = null;

        public SingleRunJint(DocumentDatabase database, RavenConfiguration configuration, ScriptRunnerJint runner, List<string> scriptsSource,
            bool executeScriptsSource = true)
            : base(database, configuration, runner, scriptsSource)
        {
            _refResolverJint = new JintPreventResolvingTasksReferenceResolver();
            var engine = new JintEngineEx(configuration, _refResolverJint);
            _engine = engine.Engine;
            EngineHandle = engine;
            JsUtils = new JavaScriptUtilsJint(runner, engine);
            JsBlittableBridge = new JsBlittableBridgeJint(engine);
            Initialize(executeScriptsSource);
        }

        protected override JsHandleJint CreateErrorAndSetLastExceptionIfNeeded(Exception e, JSValueType errorType)
        {
            return EngineHandle.CreateError(e, errorType);
        }

        protected override bool TryGetLambdaPropertyName(JsHandleJint param, out string propName)
        {
            if (param.IsObject && param.AsObject() is ScriptFunctionInstance lambda)
            {
                var functionAst = lambda.FunctionDeclaration;
                propName = functionAst.TryGetFieldFromSimpleLambdaExpression();
                return true;
            }

            propName = null;
            return false;
        }

        protected override bool ScalarToRawStringInternal(JsHandleJint param)
        {
            return false;
        }

        public override void CleanInternal()
        {
            // noop
        }

        protected override JsHandleJint CreateObjectBinder(BlittableJsonToken type, object value)
        {
            switch (type)
            {
                case BlittableJsonToken.Integer:
                case BlittableJsonToken.LazyNumber:
                case BlittableJsonToken.String:
                case BlittableJsonToken.CompressedString:
                    ObjectWrapper obj = new ObjectWrapper(_engine, value);
                    return new JsHandleJint(obj);
                default:
                    throw new InvalidOperationException("scalarToRawString(document, lambdaToField) lambda to field must return either raw numeric or raw string types");
            }
        }

        protected override string GetTypes(JsHandleJint value) => $"JintType({value.ValueType}) .NETType({value.GetType().Name})";

        protected override void SetArgs(JsHandleJint[] args, IBlittableObjectInstance<JsHandleJint> boi)
        {
            //TODO: egor check this overload
            _refResolverJint.ExplodeArgsOn(args[0].Item, (BlittableObjectInstanceJint)boi);
        }

        public override void SetArgsInternal()
        {
            if (_args.Length > 1 && _args[1].Object is BlittableObjectInstanceJint boi)
                _refResolverJint.ExplodeArgsOn(null, boi);
        }

        public override ScriptRunnerResult<JsHandleJint> Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, string method, string documentId,
            object[] args, QueryTimingsScope scope = null,
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

                EngineHandle.ForceGarbageCollection();
                if (EngineHandle.IsMemoryChecksOn && isMemorySnapshotMade)
                {
                    EngineHandle.CheckForMemoryLeaks("single_run");
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
