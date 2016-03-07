using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Threading;
using Jint;
using Jint.Native;
using Jint.Parser;
using Jint.Runtime;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents.Patch
{
    public class PatchDocument
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof (PatchDocument).FullName);

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

        public unsafe PatchResultData Apply(DocumentsOperationContext context,
            string documentId,
            long? etag,
            bool isTestOnly,
            PatchRequest patch,
            PatchRequest patchIfMissing,
            bool skipPatchIfEtagMismatch = false)
        {
            int retries = 128;
            Random rand = null;

            while (true)
            {
                using (context.OpenWriteTransaction())
                {
                    var document = _database.DocumentsStorage.Get(context, documentId);
                    if (Log.IsDebugEnabled)
                        Log.Debug(() => string.Format("Preparing to apply patch on ({0}). Document found?: {1}.", documentId, document != null));

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

                        if (Log.IsDebugEnabled)
                            Log.Debug(() => $"Got concurrent exception while tried to patch the following document: {documentId}");
                        throw new ConcurrencyException($"Could not patch document '{documentId}' because non current etag was used")
                        {
                            ActualETag = document.Etag,
                            ExpectedETag = etag.Value,
                        };
                    }

                    var patchRequest = document != null ? patch : patchIfMissing;
                    var scope = ApplyInternal(context, document, etag, isTestOnly, patchRequest);
                    var modifiedDocument = context.ReadObject(scope.ToBlittable(scope.PatchObject),
                        documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                    if (modifiedDocument == null)
                    {
                        if (Log.IsDebugEnabled)
                            Log.Debug(() => $"Preparing to apply patch on ({documentId}). DocumentDoesNotExists.");
                        return new PatchResultData
                        {
                            PatchResult = PatchResult.DocumentDoesNotExists
                        };
                    }

                    if (isTestOnly)
                    {
                        return new PatchResultData
                        {
                            PatchResult = PatchResult.Tested,
                            ModifiedDocument = modifiedDocument,
                            OriginalDocument = document?.Data,
                            DebugActions = scope.DebugActions,
                        };
                    }

                    try
                    {
                        var result = new PatchResultData
                        {
                            PatchResult = PatchResult.NotModified,
                            ModifiedDocument = modifiedDocument,
                            OriginalDocument = document?.Data,
                        };

                        if (document == null)
                        {
                            _database.DocumentsStorage.Put(context, documentId, null, modifiedDocument);
                        }
                        else
                        {
                            var isModified = document.Data.Size != modifiedDocument.Size;
                            if (isModified == false) // optimization, if size different, no need to compute hash to check
                            {
                                var originHash = Hashing.XXHash64.Calculate(document.Data.BasePointer, document.Data.Size);
                                var modifiedHash = Hashing.XXHash64.Calculate(modifiedDocument.BasePointer, modifiedDocument.Size);
                                isModified = originHash != modifiedHash;
                            }

                            if (isModified)
                            {
                                _database.DocumentsStorage.Put(context, document.Key, document.Etag, modifiedDocument);
                                result.PatchResult = PatchResult.Patched;
                            }
                        }

                        /*var docsCreatedInPatch = scope.GetPutOperations();
                        if (docsCreatedInPatch != null && docsCreatedInPatch.Count > 0)
                        {
                            foreach (var docFromPatch in docsCreatedInPatch)
                            {
                                Database.Documents.Put(docFromPatch.Key, docFromPatch.Etag, docFromPatch.DataAsJson,
                                    docFromPatch.Metadata, transactionInformation, participatingIds);
                            }
                        }*/

                        context.Transaction.Commit();
                        return result;
                    }
                    catch (ConcurrencyException)
                    {
                        if (retries-- <= 0)
                            throw;

                        if (rand == null)
                            rand = new Random();
                        Thread.Sleep(rand.Next(5, Math.Max(retries*2, 10)));
                    }
                }
            }
        }

        private bool IsModified(DynamicJsonValue modifiedDocument, DynamicJsonValue document)
        {
            throw new NotImplementedException();
        }

        private PatcherOperationScope ApplyInternal(DocumentsOperationContext context, Document document, long? etag, bool isTestOnly, PatchRequest patch)
        {
            var scope = new PatcherOperationScope(_database, context, isTestOnly)
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

                scope.PatchObject = scope.ToJsObject(jintEngine, document.Data);
                scope.ActualPatchResult = jintEngine.Invoke("ExecutePatchScript", scope.PatchObject);

                CleanupEngine(patch, jintEngine, scope);

                OutputLog(jintEngine);
                if (scope.DebugMode)
                    Debug.Add(string.Format("Statements executed: {0}", jintEngine.StatementsCount));

                ScriptsCache.CheckinScript(patch, jintEngine, scope.CustomFunctions);

                return scope;
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

        private void CleanupEngine(PatchRequest patch, Engine jintEngine, PatcherOperationScope scope)
        {
            if (patch.Values != null)
            {
                foreach (var kvp in patch.Values)
                    jintEngine.Global.Delete(kvp.Key, true);
            }

            jintEngine.Global.Delete("__document_id", true);
            RemoveEngineCustomizations(jintEngine, scope);
        }

        private void PrepareEngine(PatchRequest patch, Document document, PatcherOperationScope scope, Engine jintEngine)
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

            if (patch.Values != null)
            {
                foreach (var kvp in patch.Values)
                {
                    jintEngine.SetValue(kvp.Key, scope.ToJsInstance(jintEngine, kvp.Value));
                }
            }
            
            jintEngine.ResetStatementsCount();
        }

        private Engine CreateEngine(PatchRequest patch)
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