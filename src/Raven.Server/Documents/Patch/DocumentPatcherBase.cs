using System;
using System.Globalization;
using System.IO;
using System.Reflection;
using Jurassic;
using Jurassic.Compiler;
using Jurassic.Library;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.Config;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Exceptions;
using Sparrow.Logging;
using JavaScriptException = Raven.Client.Exceptions.Documents.Patching.JavaScriptException;

namespace Raven.Server.Documents.Patch
{
    public abstract class DocumentPatcherBase
    {
        public const int MaxRecursionDepth = 128;

        private readonly int _maxSteps;
        protected readonly int _additionalStepsPerSize;
        private readonly bool _allowScriptsToAdjustNumberOfSteps;

        private static readonly ScriptsCache ScriptsCache = new ScriptsCache();

        protected readonly Logger Logger;
        protected readonly DocumentDatabase Database;

        protected DocumentPatcherBase()
        { }

        protected DocumentPatcherBase(DocumentDatabase database)
        {
            Database = database;
            Logger = LoggingSource.Instance.GetLogger(database.Name, GetType().FullName);
            _maxSteps = database.Configuration.Patching.MaxStepsForScript;
            _additionalStepsPerSize = database.Configuration.Patching.AdditionalStepsForScriptBasedOnDocumentSize;
            _allowScriptsToAdjustNumberOfSteps = database.Configuration.Patching.AllowScriptsToAdjustNumberOfSteps;
        }

        public virtual PatchResult Apply(DocumentsOperationContext context,
                                         Document document,
                                         PatchRequest patch,
                                         BlittableJsonDocumentBuilder.UsageMode mode = BlittableJsonDocumentBuilder.UsageMode.None,
                                         IBlittableDocumentModifier modifier = null,
                                         bool debugMode = false)
        {
            if (document == null)
                return null;

            if (string.IsNullOrEmpty(patch.Script))
                throw new InvalidOperationException("Patch script must be non-null and not empty");

            using (var scope = CreateOperationScope(debugMode).Initialize(context))
            {
                ApplySingleScript(context, document.Id, document, patch, scope);

                var modifiedDocument = context.ReadObject(scope.ToBlittable(scope.PatchObject), document.Id, mode, modifier);

                var result = new PatchResult
                {
                    Status = PatchStatus.Patched,
                    OriginalDocument = document.Data,
                    ModifiedDocument = modifiedDocument
                };

                if (debugMode)
                    AddDebug(context, result, scope);

                return result;
            }
        }

        protected PatcherOperationScope CreateOperationScope(bool debugMode)
        {
            return new PatcherOperationScope(Database, debugMode)
            {
                AdditionalStepsPerSize = _additionalStepsPerSize,
                MaxSteps = _maxSteps,
                CustomFunctions = Database.Patcher.CustomFunctions
            };
        }

        protected void ApplySingleScript(DocumentsOperationContext context, string documentId, Document document, PatchRequest patch, PatcherOperationScope scope)
        {
            var run = new SingleScriptRun(this, patch, scope);
            try
            {
                run.Prepare(document?.Data?.Size ?? 0);
                run.SetDocumentId(document?.Id ?? documentId);

                scope.PatchObject = scope.ToJsObject(run.JSEngine, document);

                run.Execute();
            }
            catch (Exception errorEx)
            {
                run.HandleError(errorEx);
                throw;
            }
        }

        internal void CleanupEngine(PatchRequest patch, ScriptEngine engine, PatcherOperationScope scope)
        {
            if (patch.Values != null)
            {
                foreach (var name in patch.Values.GetPropertyNames())
                    engine.Global.Delete(name, true);
            }

            engine.Global.Delete("__document_id", true);
            RemoveEngineCustomizations(engine, scope);
        }

        public class EngineLoopIterationKeeper
        {
            public EngineLoopIterationKeeper(long maxLoopIterations)
            {
                MaxLoopIterations = maxLoopIterations;
            }
            public int LoopIterations = 0;
            public long MaxLoopIterations;

            public void OnLoopIteration()
            {
                LoopIterations++;
                if (LoopIterations == MaxLoopIterations)
                    ThrowExceededLoopIterations();
            }

            private void ThrowExceededLoopIterations()
            {
                throw new InvalidOperationException($"Javascript code exceeded a total of {MaxLoopIterations}");
            }
        }

        private void PrepareEngine(PatchRequest patch, PatcherOperationScope scope, ScriptEngine jsEngine, int documentSize)
        {
            var maxStepsSize = 1;
            if (documentSize > 0)
                maxStepsSize = GetMaxStatements(scope, jsEngine, documentSize);

            jsEngine.Global.Delete("LoadDocument", false);
            jsEngine.Global.Delete("IncreaseNumberOfAllowedStepsBy", false);

            CustomizeEngine(jsEngine, scope);

            if (jsEngine.OnLoopIterationCall == null)
                jsEngine.OnLoopIterationCall = new EngineLoopIterationKeeper(maxStepsSize).OnLoopIteration;
            
            jsEngine.SetGlobalFunction("LoadDocument", (Func<string, object>)(key => scope.LoadDocument(key, jsEngine)));

            jsEngine.SetGlobalFunction("IncreaseNumberOfAllowedStepsBy", (Action<int>)(number =>
            {
                if (_allowScriptsToAdjustNumberOfSteps == false)
                    throw new InvalidOperationException($"Cannot use 'IncreaseNumberOfAllowedStepsBy' method, because `{RavenConfiguration.GetKey(x => x.Patching.AllowScriptsToAdjustNumberOfSteps)}` is set to false.");

                var keeper = jsEngine.OnLoopIterationCallTarget as EngineLoopIterationKeeper;

                if (keeper != null)
                {
                    keeper.MaxLoopIterations += number;
                }
            }));

            if (patch.Values != null)
            {
                var prop = new BlittableJsonReaderObject.PropertyDetails();

                for (int i = 0; i < patch.Values.Count; i++)
                {
                    patch.Values.GetPropertyByIndex(i, ref prop);
                    jsEngine.SetGlobalValue(prop.Name, scope.ToJsValue(jsEngine, prop));
                }
            }

            var loopKeeper = jsEngine.OnLoopIterationCallTarget as EngineLoopIterationKeeper;
            if (loopKeeper != null) loopKeeper.MaxLoopIterations = maxStepsSize;
        }

        internal int GetMaxStatements(PatcherOperationScope scope, ScriptEngine jsEngine, int documentSize)
        {
            return _maxSteps + (documentSize * _additionalStepsPerSize);
        }

        protected string ExecutionString = @"function ExecutePatchScript(docInner){{ return (function(doc){{ {0} }}).apply(docInner); }};";

        private ScriptEngine CreateEngine(PatchRequest patch)
        {
            return CreateEngine(patch.Script, ExecutionString);
        }

        protected ScriptEngine CreateEngine(string script, string executionString)
        {
            var scriptWithProperLines = script.NormalizeLineEnding();
            // NOTE: we merged few first lines of wrapping script to make sure {0} is at line 0.
            // This will all us to show proper line number using user lines locations.
            var wrapperScript = string.Format(executionString, scriptWithProperLines);

            var jsEngine = new ScriptEngine();

#if DEBUG
            jsEngine.EnableDebugging = true;
#endif

            jsEngine.RecursionDepthLimit = MaxRecursionDepth;
            // jsEngine.NullPropagation(); todo: note that we don't have this anymore
            //AddScript(jsEngine, "Raven.Server.Documents.Patch.lodash.js");
            AddScript(jsEngine, "Raven.Server.Documents.Patch.ToJson.js");
            AddScript(jsEngine, "Raven.Server.Documents.Patch.RavenDB.js");
            

            jsEngine.Execute(wrapperScript);

            return jsEngine;
        }

        private static void AddScript(ScriptEngine scriptEngine, string ravenDatabaseJsonMapJs)
        {
            
            scriptEngine.Execute(GetFromResources(ravenDatabaseJsonMapJs));
        }

        protected abstract void CustomizeEngine(ScriptEngine engine, PatcherOperationScope scope);

        protected abstract void RemoveEngineCustomizations(ScriptEngine engine, PatcherOperationScope scope);

        private static void OutputLog(ScriptEngine engine, PatcherOperationScope scope)
        {
            var numberOfOutputs = (int)engine.GetGlobalValue("number_of_outputs");
            if (numberOfOutputs == 0)
                return;

            var arr = engine.GetGlobalValue("debug_outputs") as ArrayInstance;

            foreach (var property in arr.Properties)
            {
                var propertyKey = property.Key.ToString();
                if (propertyKey == "length")
                    continue;

                var value = property.Value;
                if (value == null)
                    continue;
                
                string output = null;

                output = value.ToString();

                if (output != null)
                    scope.DebugInfo.Add(output);
            }
            engine.CallGlobalFunction("clear_debug_outputs");
        }

        private static string GetFromResources(string resourceName)
        {
            var assembly = typeof(DocumentPatcherBase).GetTypeInfo().Assembly;
            using (var stream = assembly.GetManifestResourceStream(resourceName))
            {
                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }

        internal static void AddDebug(JsonOperationContext context, PatchResult result, PatcherOperationScope scope)
        {
            var djv = new DynamicJsonValue
            {
                ["Info"] = scope.DebugInfo,
                ["Actions"] = scope.DebugActions.GetDebugActions()
            };

            result.Debug = context.ReadObject(djv, "debug/actions");
        }

        public struct SingleScriptRun
        {
            public ScriptEngine JSEngine;

            private readonly DocumentPatcherBase _parent;
            private readonly PatchRequest _patch;
            private readonly PatcherOperationScope _scope;

            public SingleScriptRun(DocumentPatcherBase parent, PatchRequest patch, PatcherOperationScope scope)
            {
                _parent = parent;
                _patch = patch;
                _scope = scope;

                try
                {
                    JSEngine = ScriptsCache.GetEngine(parent.CreateEngine, patch, _scope.CustomFunctions);
                }
                catch (NotSupportedException e)
                {
                    throw new JavaScriptParseException("Could not parse script " + Environment.NewLine + patch.Script, e);
                }
                catch (SyntaxErrorException e)
                {
                    throw new JavaScriptParseException("Could not parse script " + Environment.NewLine + patch.Script, e);
                }
                catch (Exception e)
                {
                    throw new JavaScriptParseException("Could not parse: " + Environment.NewLine + patch.Script, e);
                }
            }

            public void Prepare(int size)
            {
                _parent.PrepareEngine(_patch, _scope, JSEngine, size);
            }

            public void Execute()
            {
                _scope.ActualPatchResult = JSEngine.CallGlobalFunction("ExecutePatchScript", _scope.PatchObject);

                _parent.CleanupEngine(_patch, JSEngine, _scope);

                OutputLog(JSEngine, _scope);
                if (_scope.DebugMode)
                {
                    var keeper = JSEngine.OnLoopIterationCallTarget as EngineLoopIterationKeeper;
                    _scope.DebugInfo.Add($"Exceution finished, loop iterations procceesed: {keeper?.LoopIterations}");
                }
            }

            public void HandleError(Exception errorEx)
            {
                if (errorEx is ConcurrencyException)
                    return;

                var keeper = JSEngine.OnLoopIterationCallTarget as EngineLoopIterationKeeper;
                if (keeper != null)
                {
                    keeper.LoopIterations = 0;
                }
                OutputLog(JSEngine, _scope);
                var errorMsg = "Unable to execute JavaScript: " + Environment.NewLine + _patch.Script + Environment.NewLine;

                var error = errorEx as JavaScriptException;
                if (error != null)
                    errorMsg += Environment.NewLine + "Error: " + Environment.NewLine + string.Join(Environment.NewLine, error.Message);
                if (_scope.DebugInfo.Items.Count != 0)
                    errorMsg += Environment.NewLine + "Debug information: " + Environment.NewLine +
                                string.Join(Environment.NewLine, _scope.DebugInfo.Items);

                if (error != null)
                    errorMsg += Environment.NewLine + "Stacktrace:" + Environment.NewLine + error.StackTrace;

                var targetEx = errorEx as TargetInvocationException;
                if (targetEx?.InnerException != null)
                    throw new JavaScriptException(errorMsg, targetEx.InnerException);

                var recursionEx = errorEx as StackOverflowException;
                if (recursionEx != null)
                    errorMsg += Environment.NewLine + "Max recursion depth is limited to: " + MaxRecursionDepth;

                throw new JavaScriptException(errorMsg, errorEx);
            }

            public void SetDocumentId(string documentId)
            {
                JSEngine.SetGlobalValue("__document_id", documentId);
            }
        }

    }
}
