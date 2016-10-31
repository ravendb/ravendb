using System;
using System.Globalization;
using System.IO;
using System.Reflection;
using Jint;
using Jint.Native;
using Jint.Parser;
using Jint.Runtime;
using Raven.Abstractions.Logging;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Voron.Exceptions;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Documents.Patch
{
    public class PatchDocument
    {
        protected static Logger _logger;
        private const int MaxRecursionDepth = 128;
        private readonly int maxSteps;
        private readonly int additionalStepsPerSize;
        private readonly bool allowScriptsToAdjustNumberOfSteps;

        private static readonly ScriptsCache ScriptsCache = new ScriptsCache();

        private readonly DocumentDatabase _database;

        public PatchDocument(DocumentDatabase database)
        {
            _database = database;
            _logger = LoggingSource.Instance.GetLogger<PatchDocument>(database.Name);
            maxSteps = database.Configuration.Patching.MaxStepsForScript;
            additionalStepsPerSize = database.Configuration.Patching.AdditionalStepsForScriptBasedOnDocumentSize;
            allowScriptsToAdjustNumberOfSteps = database.Configuration.Patching.AllowScriptsToAdjustNumberOfSteps;
        }

        public virtual PatchResultData Apply(DocumentsOperationContext context, Document document, PatchRequest patch)
        {
            if (document == null)
                return null;

            if (string.IsNullOrEmpty(patch.Script))
                throw new InvalidOperationException("Patch script must be non-null and not empty");

            var scope = ApplySingleScript(context, document, false, patch);
            var modifiedDocument = context.ReadObject(scope.ToBlittable(scope.PatchObject.AsObject()), document.Key); /* TODO: Should not use BlittableJsonDocumentBuilder.UsageMode.ToDisk? */
            return new PatchResultData
            {
                ModifiedDocument = modifiedDocument ?? document.Data,
                DebugInfo = scope.DebugInfo,
            };
        }

        public unsafe PatchResultData Apply(DocumentsOperationContext context,
            string documentKey,
            long? etag,
            PatchRequest patch,
            PatchRequest patchIfMissing,
            bool isTestOnly = false,
            bool skipPatchIfEtagMismatch = false)
        {
            var document = _database.DocumentsStorage.Get(context, documentKey);
            if (_logger.IsInfoEnabled)
                _logger.Info(string.Format("Preparing to apply patch on ({0}). Document found?: {1}.", documentKey, document != null));

            if (etag.HasValue && document != null && document.Etag != etag.Value)
            {
                System.Diagnostics.Debug.Assert(document.Etag > 0);

                if (skipPatchIfEtagMismatch)
                {
                    return new PatchResultData
                    {
                        PatchResult = PatchResult.Skipped
                    };
                }

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Got concurrent exception while tried to patch the following document: {documentKey}");
                throw new ConcurrencyException($"Could not patch document '{documentKey}' because non current etag was used")
                {
                    ActualETag = document.Etag,
                    ExpectedETag = etag.Value,
                };
            }

            var patchRequest = patch;
            if (document == null)
            {
                if (patchIfMissing == null)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Tried to patch a not exists document and patchIfMissing is null");

                    return new PatchResultData
                    {
                        PatchResult = PatchResult.DocumentDoesNotExists
                    };
                }
                patchRequest = patchIfMissing;
            }
            var scope = ApplySingleScript(context, document, isTestOnly, patchRequest);
            var modifiedDocument = context.ReadObject(scope.ToBlittable(scope.PatchObject.AsObject()),
                documentKey, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

            var result = new PatchResultData
            {
                PatchResult = PatchResult.NotModified,
                OriginalDocument = document?.Data,
                DebugInfo = scope.DebugInfo,
            };

            if (modifiedDocument == null)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"After applying patch, modifiedDocument is null and document is null? {document == null}");

                result.PatchResult = PatchResult.Skipped;
                return result;
            }

            if (isTestOnly)
            {
                return new PatchResultData
                {
                    PatchResult = PatchResult.Tested,
                    OriginalDocument = document?.Data,
                    ModifiedDocument = modifiedDocument,
                    DebugActions = scope.DebugActions.GetDebugActions(),
                    DebugInfo = scope.DebugInfo,
                };
            }

            if (document == null)
            {
                _database.DocumentsStorage.Put(context, documentKey, null, modifiedDocument);
            }
            else
            {
                var isModified = document.Data.Size != modifiedDocument.Size;
                if (isModified == false) // optimization, if size different, no need to compute hash to check
                {
                    var originHash = Hashing.XXHash64.Calculate(document.Data.BasePointer, (ulong)document.Data.Size);
                    var modifiedHash = Hashing.XXHash64.Calculate(modifiedDocument.BasePointer, (ulong)modifiedDocument.Size);
                    isModified = originHash != modifiedHash;
                }

                if (isModified)
                {
                    _database.DocumentsStorage.Put(context, document.Key, document.Etag, modifiedDocument);
                    result.PatchResult = PatchResult.Patched;
                }
            }

            return result;
        }

        protected PatcherOperationScope ApplySingleScript(DocumentsOperationContext context, Document document, bool isTestOnly, PatchRequest patch)
        {
            var scope = new PatcherOperationScope(_database, context, isTestOnly)
            {
                AdditionalStepsPerSize = additionalStepsPerSize,
                MaxSteps = maxSteps,
            };

            Engine jintEngine;
            try
            {
                jintEngine = ScriptsCache.GetEngine(CreateEngine, patch, scope.CustomFunctions);
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

                scope.PatchObject = scope.ToJsObject(jintEngine, document.Data);
                scope.ActualPatchResult = jintEngine.Invoke("ExecutePatchScript", scope.PatchObject);

                CleanupEngine(patch, jintEngine, scope);

                OutputLog(jintEngine, scope);
                if (scope.DebugMode)
                    scope.DebugInfo.Add(string.Format("Statements executed: {0}", jintEngine.StatementsCount));

                return scope;
            }
            catch (ConcurrencyException)
            {
                throw;
            }
            catch (Exception errorEx)
            {
                jintEngine.ResetStatementsCount();

                OutputLog(jintEngine, scope);
                var errorMsg = "Unable to execute JavaScript: " + Environment.NewLine + patch.Script + Environment.NewLine;
                var error = errorEx as JavaScriptException;
                if (error != null)
                    errorMsg += Environment.NewLine + "Error: " + Environment.NewLine + string.Join(Environment.NewLine, error.Error);
                if (scope.DebugInfo.Items.Count != 0)
                    errorMsg += Environment.NewLine + "Debug information: " + Environment.NewLine +
                                string.Join(Environment.NewLine, scope.DebugInfo.Items);

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

        private void PrepareEngine(PatchRequest patch, Document document, PatcherOperationScope scope, Engine jintEngine)
        {
            int totalScriptSteps = 0;
            if (document.Data.Size != 0)
            {
                totalScriptSteps = maxSteps + (document.Data.Size * additionalStepsPerSize);
                jintEngine.Options.MaxStatements(totalScriptSteps);
            }

            
            jintEngine.Global.Delete("LoadDocument", false);
            jintEngine.Global.Delete("IncreaseNumberOfAllowedStepsBy", false);

            CustomizeEngine(jintEngine, scope);
            
            jintEngine.SetValue("LoadDocument", (Func<string, JsValue>)(key => scope.LoadDocument(key, jintEngine, ref totalScriptSteps)));
            jintEngine.SetValue("__document_id", document.Key);

            jintEngine.SetValue("IncreaseNumberOfAllowedStepsBy", (Action<int>)(number =>
            {
                if (allowScriptsToAdjustNumberOfSteps == false)
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

        private Engine CreateEngine(PatchRequest patch)
        {
            var scriptWithProperLines = patch.Script.NormalizeLineEnding();
            // NOTE: we merged few first lines of wrapping script to make sure {0} is at line 0.
            // This will all us to show proper line number using user lines locations.
            var wrapperScript = string.Format(@"function ExecutePatchScript(docInner){{ return (function(doc){{ {0} }}).apply(docInner); }};", scriptWithProperLines);

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
                    scope.DebugInfo.Add(output);
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