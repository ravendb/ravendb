using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Reflection;
using Jint;
using Jint.Native;
using Jint.Parser;
using Jint.Runtime;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Server.Extensions;

namespace Raven.Server.Documents.Patch
{
    public class PatchDocument
    {
        private const int MaxRecursionDepth = 128;
        private readonly int maxSteps;
        private readonly int additionalStepsPerSize;
        private readonly bool allowScriptsToAdjustNumberOfSteps;
        private int totalScriptSteps;

        private static readonly ScriptsCache ScriptsCache = new ScriptsCache();
        public readonly List<string> Debug = new List<string>();

        private readonly DocumentDatabase _database;

        public PatchDocument(DocumentDatabase database)
        {
            _database = database;
            maxSteps = database.Configuration.Patching.MaxStepsForScript;
            additionalStepsPerSize = database.Configuration.Patching.AdditionalStepsForScriptBasedOnDocumentSize;
            allowScriptsToAdjustNumberOfSteps = database.Configuration.Patching.AllowScriptsToAdjustNumberOfSteps;
        }

        public void Apply(Document document, long? etag, ScriptedPatchRequest patch, bool isTestOnly)
        {
            var scope = new PatcherOperationScope(isTestOnly)
            {
                AdditionalStepsPerSize = additionalStepsPerSize,
                MaxSteps = maxSteps,
            };

            Engine jintEngine;
            try
            {
                jintEngine = ScriptsCache.CheckoutScript(CreateEngine, patch, scope.CustomFunctions);
            }
            catch (NotSupportedException e)
            {
                throw new ParseException("Could not parse script", e);
            }
            catch (JavaScriptException e)
            {
                throw new ParseException("Could not parse script", e);
            }
            catch (Exception e)
            {
                throw new ParseException("Could not parse: " + Environment.NewLine + patch.Script, e);
            }

            try
            {
                PrepareEngine(patch, document, scope, jintEngine);

                var jsObject = scope.ToJsObject(jintEngine, document);
                scope.ActualPatchResult = jintEngine.Invoke("ExecutePatchScript", jsObject);

                CleanupEngine(patch, jintEngine, scope);

                OutputLog(jintEngine);
                if (scope.DebugMode)
                    Debug.Add(string.Format("Statements executed: {0}", jintEngine.StatementsCount));

                ScriptsCache.CheckinScript(patch, jintEngine, scope.CustomFunctions);

                throw new NotImplementedException();
                // return scope.ConvertReturnValue(jsObject);
            }
            catch (ConcurrencyException)
            {
                throw;
            }
            catch (Exception errorEx)
            {
                jintEngine.ResetStatementsCount();

                OutputLog(jintEngine);
                var errorMsg = "Unable to execute JavaScript: " + Environment.NewLine + patch.Script + Environment.NewLine;
                var error = errorEx as JavaScriptException;
                if (error != null)
                    errorMsg += Environment.NewLine + "Error: " + Environment.NewLine + string.Join(Environment.NewLine, error.Error);
                if (Debug.Count != 0)
                    errorMsg += Environment.NewLine + "Debug information: " + Environment.NewLine +
                                string.Join(Environment.NewLine, Debug);

                if (error != null)
                    errorMsg += Environment.NewLine + "Stacktrace:" + Environment.NewLine + error.CallStack;

                var targetEx = errorEx as TargetInvocationException;
                if (targetEx != null && targetEx.InnerException != null)
                    throw new InvalidOperationException(errorMsg, targetEx.InnerException);

                var recursionEx = errorEx as RecursionDepthOverflowException;
                if (recursionEx != null)
                    errorMsg += Environment.NewLine + "Max recursion depth is limited to: " + MaxRecursionDepth;

                throw new InvalidOperationException(errorMsg, errorEx);
            }
        }

        private void CleanupEngine(ScriptedPatchRequest patch, Engine jintEngine, PatcherOperationScope scope)
        {
            foreach (var kvp in patch.Values)
                jintEngine.Global.Delete(kvp.Key, true);

            jintEngine.Global.Delete("__document_id", true);
            RemoveEngineCustomizations(jintEngine, scope);
        }

        private void PrepareEngine(ScriptedPatchRequest patch, Document document, PatcherOperationScope scope, Engine jintEngine)
        {
            if (document.Data.Size != 0)
            {
                totalScriptSteps = maxSteps + (document.Data.Size * additionalStepsPerSize);
                jintEngine.Options.MaxStatements(totalScriptSteps);
            }

            jintEngine.Global.Delete("PutDocument", false);
            jintEngine.Global.Delete("LoadDocument", false);
            jintEngine.Global.Delete("DeleteDocument", false);
            jintEngine.Global.Delete("IncreaseNumberOfAllowedStepsBy", false);

            CustomizeEngine(jintEngine, scope);

            jintEngine.SetValue("PutDocument", (Func<string, object, object, string>)((key, data, metadata) => scope.PutDocument(key, data, metadata, jintEngine)));
            jintEngine.SetValue("LoadDocument", (Func<string, JsValue>)(key => scope.LoadDocument(key, jintEngine, ref totalScriptSteps)));
            jintEngine.SetValue("DeleteDocument", (Action<string>)scope.DeleteDocument);
            jintEngine.SetValue("__document_id", document.Key);

            jintEngine.SetValue("IncreaseNumberOfAllowedStepsBy", (Action<int>)(number =>
            {
                if (allowScriptsToAdjustNumberOfSteps == false)
                    throw new InvalidOperationException("Cannot use 'IncreaseNumberOfAllowedStepsBy' method, because `Raven/AllowScriptsToAdjustNumberOfSteps` is set to false.");

                scope.MaxSteps += number;
                totalScriptSteps += number; // TODO: Review this line, this is a change from v3.5
                jintEngine.Options.MaxStatements(totalScriptSteps);
            }));

            foreach (var kvp in patch.Values)
            {
                jintEngine.SetValue(kvp.Key, scope.ToJsInstance(jintEngine, kvp.Value));
            }

            jintEngine.ResetStatementsCount();
        }

        private Engine CreateEngine(ScriptedPatchRequest patch)
        {
            var scriptWithProperLines = patch.Script.NormalizeLineEnding();
            // NOTE: we merged few first lines of wrapping script to make sure {0} is at line 0.
            // This will all us to show proper line number using user lines locations.
            var wrapperScript = string.Format(@"function ExecutePatchScript(docInner){{ (function(doc){{ {0} }}).apply(docInner); }};", scriptWithProperLines);

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

            jintEngine.Options.MaxStatements(maxSteps);

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

        protected virtual void CustomizeEngine(Engine engine, PatcherOperationScope scope)
        {
        }

        protected virtual void RemoveEngineCustomizations(Engine engine, PatcherOperationScope scope)
        {
        }

        private void OutputLog(Engine engine)
        {
            var arr = engine.GetValue("debug_outputs");
            if (arr == JsValue.Null || arr.IsArray() == false)
                return;

            foreach (var property in arr.AsArray().GetOwnProperties())
            {
                if (property.Key == "length")
                    continue;

                var jsInstance = property.Value.Value;
                if (!jsInstance.HasValue)
                    continue;

                var value = jsInstance.Value;
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
                    Debug.Add(output);
            }

            engine.Invoke("clear_debug_outputs");
        }

        private static string GetFromResources(string resourceName)
        {
            var assembly = typeof(PatchDocument).Assembly();
            using (Stream stream = assembly.GetManifestResourceStream(resourceName))
            {
                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }
    }
}