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
    public class PatchDocument
    {
        protected static Logger _logger;
        private const int MaxRecursionDepth = 128;
        private readonly int _maxSteps;
        protected readonly int _additionalStepsPerSize;
        private readonly bool _allowScriptsToAdjustNumberOfSteps;

        private static readonly ScriptsCache ScriptsCache = new ScriptsCache();

        protected readonly DocumentDatabase _database;

        public PatchDocument(DocumentDatabase database)
        {
            _database = database;
            _logger = LoggingSource.Instance.GetLogger<PatchDocument>(database.Name);
            _maxSteps = database.Configuration.Patching.MaxStepsForScript;
            _additionalStepsPerSize = database.Configuration.Patching.AdditionalStepsForScriptBasedOnDocumentSize;
            _allowScriptsToAdjustNumberOfSteps = database.Configuration.Patching.AllowScriptsToAdjustNumberOfSteps;
        }

        public virtual PatchResult Apply(DocumentsOperationContext context, Document document, PatchRequest patch)
        {
            if (document == null)
                return null;

            if (string.IsNullOrEmpty(patch.Script))
                throw new InvalidOperationException("Patch script must be non-null and not empty");

            var scope = ApplySingleScript(context, document, patch, debugMode: false);
            var modifiedDocument = context.ReadObject(scope.ToBlittable(scope.PatchObject.AsObject()), document.Key); /* TODO: Should not use BlittableJsonDocumentBuilder.UsageMode.ToDisk? */

            return new PatchResult
            {
                Status = PatchStatus.Patched,
                OriginalDocument = document.Data,
                ModifiedDocument = modifiedDocument
            };
        }

        public PatchResult Apply(
            DocumentsOperationContext context,
            string documentKey,
            long? etag,
            PatchRequest patch,
            PatchRequest patchIfMissing,
            bool skipPatchIfEtagMismatch,
            bool debugMode)
        {
            if (documentKey == null)
                throw new ArgumentNullException(nameof(documentKey));

            if (string.IsNullOrWhiteSpace(patch.Script))
                throw new InvalidOperationException("Patch script must be non-null and not empty.");

            if (patchIfMissing != null && string.IsNullOrWhiteSpace(patchIfMissing.Script))
                throw new InvalidOperationException("Patch script must be non-null and not empty.");

            var document = _database.DocumentsStorage.Get(context, documentKey);
            if (etag.HasValue)
            {
                if (document == null && etag.Value != 0)
                {
                    if (skipPatchIfEtagMismatch)
                        return new PatchResult { Status = PatchStatus.Skipped };

                    throw new ConcurrencyException($"Could not patch document '{documentKey}' because non current etag was used")
                    {
                        ActualETag = 0,
                        ExpectedETag = etag.Value,
                    };
                }

                if (document != null && document.Etag != etag.Value)
                {
                    if (skipPatchIfEtagMismatch)
                        return new PatchResult { Status = PatchStatus.Skipped };

                    throw new ConcurrencyException($"Could not patch document '{documentKey}' because non current etag was used")
                    {
                        ActualETag = document.Etag,
                        ExpectedETag = etag.Value,
                    };
                }
            }

            if (document == null && patchIfMissing == null)
                return new PatchResult { Status = PatchStatus.DocumentDoesNotExist };

            var patchRequest = patch;
            if (document == null)
                patchRequest = patchIfMissing;

            var scope = ApplySingleScript(context, document, patchRequest, debugMode);
            var modifiedDocument = context.ReadObject(scope.ToBlittable(scope.PatchObject.AsObject()), documentKey, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

            var result = new PatchResult
            {
                Status = PatchStatus.NotModified,
                OriginalDocument = document?.Data,
                ModifiedDocument = modifiedDocument
            };

            if (debugMode)
            {
                var djv = new DynamicJsonValue
                {
                    ["Info"] = scope.DebugInfo,
                    ["Actions"] = scope.DebugActions.GetDebugActions()
                };

                result.Debug = context.ReadObject(djv, "debug/actions");
            }

            if (modifiedDocument == null)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"After applying patch, modifiedDocument is null and document is null? {document == null}");

                result.Status = PatchStatus.Skipped;
                return result;
            }

            var putResult = new DocumentsStorage.PutOperationResults();

            if (document == null)
            {
                putResult = _database.DocumentsStorage.Put(context, documentKey, null, modifiedDocument);
                result.Status = PatchStatus.Created;
            }
            else
            {
                if (document.Data.Equals(modifiedDocument) == false)
                {
                    putResult = _database.DocumentsStorage.Put(context, document.Key, document.Etag, modifiedDocument);
                    result.Status = PatchStatus.Patched;
                }
            }

            if (putResult.Etag != 0)
            {
                result.Etag = putResult.Etag;
                result.Collection = putResult.Collection.Name;
            }

            return result;
        }

        protected PatcherOperationScope GenerateDefaultOperationScope(DocumentsOperationContext context, bool debugMode)
        {
            return new PatcherOperationScope(_database, context, debugMode)
            {
                AdditionalStepsPerSize = _additionalStepsPerSize,
                MaxSteps = _maxSteps
            };
        }

        public struct SingleScriptRun
        {
            private readonly PatchDocument _parent;
            private readonly PatchRequest _patch;
            public Engine JintEngine;
            public PatcherOperationScope Scope;

            public SingleScriptRun(PatchDocument parent, DocumentsOperationContext context, PatchRequest patch, bool debugMode, PatcherOperationScope externalScope = null)
            {
                _parent = parent;
                _patch = patch;
                Scope = externalScope ?? parent.GenerateDefaultOperationScope(context, debugMode);

                try
                {
                    JintEngine = ScriptsCache.GetEngine(parent.CreateEngine, patch, Scope.CustomFunctions);
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
                _parent.PrepareEngine(_patch, Scope, JintEngine, size);
            }

            public void Execute()
            {
                Scope.ActualPatchResult = JintEngine.Invoke("ExecutePatchScript", Scope.PatchObject);

                _parent.CleanupEngine(_patch, JintEngine, Scope);

                _parent.OutputLog(JintEngine, Scope);
                if (Scope.DebugMode)
                    Scope.DebugInfo.Add(string.Format("Statements executed: {0}", JintEngine.StatementsCount));
            }

            public void HandleError(Exception errorEx)
            {
                if (errorEx is ConcurrencyException)
                    return;

                JintEngine.ResetStatementsCount();

                _parent.OutputLog(JintEngine, Scope);
                var errorMsg = "Unable to execute JavaScript: " + Environment.NewLine + _patch.Script + Environment.NewLine;
                var error = errorEx as Jint.Runtime.JavaScriptException;
                if (error != null)
                    errorMsg += Environment.NewLine + "Error: " + Environment.NewLine + string.Join(Environment.NewLine, error.Error);
                if (Scope.DebugInfo.Items.Count != 0)
                    errorMsg += Environment.NewLine + "Debug information: " + Environment.NewLine +
                                string.Join(Environment.NewLine, Scope.DebugInfo.Items);

                if (error != null)
                    errorMsg += Environment.NewLine + "Stacktrace:" + Environment.NewLine + error.CallStack;

                var targetEx = errorEx as TargetInvocationException;
                if (targetEx != null && targetEx.InnerException != null)
                    throw new JavaScriptException(errorMsg, targetEx.InnerException);

                var recursionEx = errorEx as RecursionDepthOverflowException;
                if (recursionEx != null)
                    errorMsg += Environment.NewLine + "Max recursion depth is limited to: " + MaxRecursionDepth;

                throw new JavaScriptException(errorMsg, errorEx);
            }
        }

        protected PatcherOperationScope ApplySingleScript(DocumentsOperationContext context, Document document, PatchRequest patch, bool debugMode, PatcherOperationScope externalScope = null)
        {
            var run = new SingleScriptRun(this, context, patch, debugMode,externalScope);
            try
            {
                run.Prepare(document?.Data?.Size ?? 0);
                SetupInputs(document, run.Scope, run.JintEngine);
                run.Execute();
                return run.Scope;
            }
            catch (Exception errorEx)
            {
                run.HandleError(errorEx);
                throw;
            }
        }

        protected void SetupInputs(Document document, PatcherOperationScope scope, Engine jintEngine)
        {
            jintEngine.SetValue("__document_id", document.Key);
            scope.PatchObject = scope.ToJsObject(jintEngine, document);
        }

        private void CleanupEngine(PatchRequest patch, Engine jintEngine, PatcherOperationScope scope)
        {
            if (patch.Values != null)
            {
                foreach (var name in patch.Values.GetPropertyNames())
                    jintEngine.Global.Delete(name, true);
            }

            jintEngine.Global.Delete("__document_id", true);
            RemoveEngineCustomizations(jintEngine, scope);
        }

        private void PrepareEngine(PatchRequest patch, PatcherOperationScope scope, Engine jintEngine, int documentSize)
        {
            int totalScriptSteps = 0;
            if (documentSize != 0)
            {
                totalScriptSteps = _maxSteps + (documentSize * _additionalStepsPerSize);
                jintEngine.Options.MaxStatements(totalScriptSteps);
            }


            jintEngine.Global.Delete("LoadDocument", false);
            jintEngine.Global.Delete("IncreaseNumberOfAllowedStepsBy", false);

            CustomizeEngine(jintEngine, scope);

            jintEngine.SetValue("LoadDocument", (Func<string, JsValue>)(key => scope.LoadDocument(key, jintEngine, ref totalScriptSteps)));

            jintEngine.SetValue("IncreaseNumberOfAllowedStepsBy", (Action<int>)(number =>
            {
                if (_allowScriptsToAdjustNumberOfSteps == false)
                    throw new InvalidOperationException("Cannot use 'IncreaseNumberOfAllowedStepsBy' method, because `Raven/AllowScriptsToAdjustNumberOfSteps` is set to false.");

                scope.MaxSteps += number;
                totalScriptSteps += number;
                jintEngine.Options.MaxStatements(totalScriptSteps);
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

        protected string ExecutionString =
            @"function ExecutePatchScript(docInner){{ return (function(doc){{ {0} }}).apply(docInner); }};";
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

        protected virtual void CustomizeEngine(Engine engine, PatcherOperationScope scope)
        {
            engine.Global.Delete("PutDocument", false);
            engine.Global.Delete("DeleteDocument", false);
            engine.SetValue("PutDocument", (Func<string, JsValue, JsValue, JsValue, string>)((key, data, metadata, etag) => scope.PutDocument(key, data, metadata, etag, engine)));
            engine.SetValue("DeleteDocument", (Action<string>)scope.DeleteDocument);
        }

        protected virtual void RemoveEngineCustomizations(Engine engine, PatcherOperationScope scope)
        {
        }

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
            var assembly = typeof(PatchDocument).GetTypeInfo().Assembly;
            using (var stream = assembly.GetManifestResourceStream(resourceName))
            {
                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }
    }
}