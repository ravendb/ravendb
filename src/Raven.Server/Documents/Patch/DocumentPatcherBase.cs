using System;
using System.Globalization;
using System.IO;
using System.Reflection;
using Jint;
using Jint.Native;
using Jint.Parser;
using Jint.Runtime;
using Raven.Client.Documents.Exceptions.Patching;
using Raven.Client.Documents.Operations;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Exceptions;
using Sparrow.Logging;
using JavaScriptException = Raven.Client.Documents.Exceptions.Patching.JavaScriptException;

namespace Raven.Server.Documents.Patch
{
    public abstract class DocumentPatcherBase
    {
        private const int MaxRecursionDepth = 128;

        private readonly int _maxSteps;
        protected readonly int _additionalStepsPerSize;
        private readonly bool _allowScriptsToAdjustNumberOfSteps;

        private static readonly ScriptsCache ScriptsCache = new ScriptsCache();

        protected readonly Logger Logger;
        protected readonly DocumentDatabase Database;

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

                var modifiedDocument = context.ReadObject(scope.ToBlittable(scope.PatchObject.AsObject()), document.Id, mode, modifier);

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

                scope.PatchObject = scope.ToJsObject(run.JintEngine, document);

                run.Execute();
            }
            catch (Exception errorEx)
            {
                run.HandleError(errorEx);
                throw;
            }
        }

        internal void CleanupEngine(PatchRequest patch, Engine engine, PatcherOperationScope scope)
        {
            if (patch.Values != null)
            {
                foreach (var name in patch.Values.GetPropertyNames())
                    engine.Global.Delete(name, true);
            }

            engine.Global.Delete("__document_id", true);
            RemoveEngineCustomizations(engine, scope);
        }

        private void PrepareEngine(PatchRequest patch, PatcherOperationScope scope, Engine jintEngine, int documentSize)
        {
            scope.TotalScriptSteps = 0;
            if (documentSize > 0)
                SetMaxStatements(scope, jintEngine, documentSize);

            jintEngine.Global.Delete("LoadDocument", false);
            jintEngine.Global.Delete("IncreaseNumberOfAllowedStepsBy", false);

            CustomizeEngine(jintEngine, scope);

            jintEngine.SetValue("LoadDocument", (Func<string, JsValue>)(key => scope.LoadDocument(key, jintEngine, ref scope.TotalScriptSteps)));

            jintEngine.SetValue("IncreaseNumberOfAllowedStepsBy", (Action<int>)(number =>
            {
                if (_allowScriptsToAdjustNumberOfSteps == false)
                    throw new InvalidOperationException("Cannot use 'IncreaseNumberOfAllowedStepsBy' method, because `Raven/AllowScriptsToAdjustNumberOfSteps` is set to false.");

                scope.MaxSteps += number;
                scope.TotalScriptSteps += number;
                jintEngine.Options.MaxStatements(scope.TotalScriptSteps);
            }));

            if (patch.Values != null)
            {
                var prop = new BlittableJsonReaderObject.PropertyDetails();

                for (int i = 0; i < patch.Values.Count; i++)
                {
                    patch.Values.GetPropertyByIndex(i, ref prop);
                    jintEngine.SetValue(prop.Name, scope.ToJsValue(jintEngine, prop.Value, prop.Token));
                }
            }

            jintEngine.ResetStatementsCount();
        }

        internal void SetMaxStatements(PatcherOperationScope scope, Engine jintEngine, int documentSize)
        {
            scope.TotalScriptSteps = _maxSteps + (documentSize * _additionalStepsPerSize);
            jintEngine.Options.MaxStatements(scope.TotalScriptSteps);
        }

        protected string ExecutionString = @"function ExecutePatchScript(docInner){{ return (function(doc){{ {0} }}).apply(docInner); }};";

        private Engine CreateEngine(PatchRequest patch)
        {
            var scriptWithProperLines = patch.Script.NormalizeLineEnding();
            // NOTE: we merged few first lines of wrapping script to make sure {0} is at line 0.
            // This will all us to show proper line number using user lines locations.
            var wrapperScript = string.Format(ExecutionString, scriptWithProperLines);

            var jintEngine = new Engine(cfg =>
            {
#if DEBUG
                cfg.AllowDebuggerStatement();
#else
                cfg.AllowDebuggerStatement(false);
#endif
                cfg.LimitRecursion(MaxRecursionDepth);
                cfg.NullPropagation();
                cfg.MaxStatements(int.MaxValue); // allow lodash to load
            });

            AddScript(jintEngine, "Raven.Server.Documents.Patch.lodash.js");
            AddScript(jintEngine, "Raven.Server.Documents.Patch.ToJson.js");
            AddScript(jintEngine, "Raven.Server.Documents.Patch.RavenDB.js");

            jintEngine.Options.MaxStatements(_maxSteps);

            jintEngine.Execute(wrapperScript, new ParserOptions
            {
                Source = "main.js"
            });

            return jintEngine;
        }

        private static void AddScript(Engine jintEngine, string ravenDatabaseJsonMapJs)
        {
            jintEngine.Execute(GetFromResources(ravenDatabaseJsonMapJs), new ParserOptions
            {
                Source = ravenDatabaseJsonMapJs
            });
        }

        protected abstract void CustomizeEngine(Engine engine, PatcherOperationScope scope);

        protected abstract void RemoveEngineCustomizations(Engine engine, PatcherOperationScope scope);

        private void OutputLog(Engine engine, PatcherOperationScope scope)
        {
            var numberOfOutputs = (int)engine.GetValue("number_of_outputs").AsNumber();
            if (numberOfOutputs == 0)
                return;

            var arr = engine.GetValue("debug_outputs").AsArray();

            foreach (var property in arr.GetOwnProperties())
            {
                if (property.Key == "length")
                    continue;

                var value = property.Value.Value;
                if (value == null)
                    continue;

                string output = null;
                switch (value.Type)
                {
                    case Types.Boolean:
                        output = value.AsBoolean().ToString();
                        break;
                    case Types.Null:
                    case Types.Undefined:
                        output = value.ToString();
                        break;
                    case Types.Number:
                        output = value.AsNumber().ToString(CultureInfo.InvariantCulture);
                        break;
                    case Types.String:
                        output = value.AsString();
                        break;
                }

                if (output != null)
                    scope.DebugInfo.Add(output);
            }

            engine.Invoke("clear_debug_outputs");
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
            public Engine JintEngine;

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
                    JintEngine = ScriptsCache.GetEngine(parent.CreateEngine, patch, _scope.CustomFunctions);
                }
                catch (NotSupportedException e)
                {
                    throw new JavaScriptParseException("Could not parse script", e);
                }
                catch (Jint.Runtime.JavaScriptException e)
                {
                    throw new JavaScriptParseException("Could not parse script", e);
                }
                catch (Exception e)
                {
                    throw new JavaScriptParseException("Could not parse: " + Environment.NewLine + patch.Script, e);
                }
            }

            public void Prepare(int size)
            {
                _parent.PrepareEngine(_patch, _scope, JintEngine, size);
            }

            public void Execute()
            {
                _scope.ActualPatchResult = JintEngine.Invoke("ExecutePatchScript", _scope.PatchObject);

                _parent.CleanupEngine(_patch, JintEngine, _scope);

                _parent.OutputLog(JintEngine, _scope);
                if (_scope.DebugMode)
                    _scope.DebugInfo.Add(string.Format("Statements executed: {0}", JintEngine.StatementsCount));
            }

            public void HandleError(Exception errorEx)
            {
                if (errorEx is ConcurrencyException)
                    return;

                JintEngine.ResetStatementsCount();

                _parent.OutputLog(JintEngine, _scope);
                var errorMsg = "Unable to execute JavaScript: " + Environment.NewLine + _patch.Script + Environment.NewLine;
                var error = errorEx as Jint.Runtime.JavaScriptException;
                if (error != null)
                    errorMsg += Environment.NewLine + "Error: " + Environment.NewLine + string.Join(Environment.NewLine, error.Error);
                if (_scope.DebugInfo.Items.Count != 0)
                    errorMsg += Environment.NewLine + "Debug information: " + Environment.NewLine +
                                string.Join(Environment.NewLine, _scope.DebugInfo.Items);

                if (error != null)
                    errorMsg += Environment.NewLine + "Stacktrace:" + Environment.NewLine + error.CallStack;

                var targetEx = errorEx as TargetInvocationException;
                if (targetEx?.InnerException != null)
                    throw new JavaScriptException(errorMsg, targetEx.InnerException);

                var recursionEx = errorEx as RecursionDepthOverflowException;
                if (recursionEx != null)
                    errorMsg += Environment.NewLine + "Max recursion depth is limited to: " + MaxRecursionDepth;

                throw new JavaScriptException(errorMsg, errorEx);
            }

            public void SetDocumentId(string documentId)
            {
                JintEngine.SetValue("__document_id", documentId);
            }
        }

    }
}