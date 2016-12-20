using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using Jint;
using Jint.Native;
using Jint.Parser;
using Jint.Parser.Ast;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Raven.Client.Replication.Messages;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Data.Tables;
using Voron.Exceptions;
using TypeExtensions = System.Reflection.TypeExtensions;

namespace Raven.Server.Documents.Patch
{
    public class PatchConflict
    {

        protected static Logger _logger;
        private const int MaxRecursionDepth = 128;
        private readonly int maxSteps;
        private readonly int additionalStepsPerSize;
        private readonly bool allowScriptsToAdjustNumberOfSteps;

        private static readonly ScriptsCache ScriptsCache = new ScriptsCache();

        private readonly DocumentDatabase _database;
        private readonly List<DocumentConflict> _docs = new List<DocumentConflict>();
        private readonly string _key;
        private readonly bool _hasTombstone = false;

        public PatchConflict(DocumentDatabase database, IReadOnlyCollection<DocumentConflict> docs,
            string key)
        {
            _database = database;
            foreach (var doc in docs)
            {
                if (doc.Doc != null)
                {
                    _docs.Add(doc);
                }
            }
            _hasTombstone = docs.Any(doc => doc.Doc == null);

            _key = key;
            _logger = LoggingSource.Instance.GetLogger<PatchConflict>(database.Name);
            maxSteps = database.Configuration.Patching.MaxStepsForScript;
            additionalStepsPerSize = database.Configuration.Patching.AdditionalStepsForScriptBasedOnDocumentSize;
            allowScriptsToAdjustNumberOfSteps = database.Configuration.Patching.AllowScriptsToAdjustNumberOfSteps;
           
        }

        public virtual PatchResultData Apply(DocumentsOperationContext context, PatchRequest patch, string collection)
        {           
            if (string.IsNullOrEmpty(patch.Script))
                throw new InvalidOperationException("Patch script must be non-null and not empty");

            var scope = ApplySingleScript(context,  true, patch,collection);
            
            var resolvedDocument = TryParse(context,scope);
            return new PatchResultData
            {
                ModifiedDocument = resolvedDocument,
                DebugInfo = scope.DebugInfo
            };
        }

        private BlittableJsonReaderObject TryParse(DocumentsOperationContext context, PatcherOperationScope scope)
        {            
            try
            {
                var obj = scope.ActualPatchResult.AsObject();
                return context.ReadObject(scope.ToBlittable(obj), _key);
            }
            catch (ArgumentException ex)
            {
                if (scope.ActualPatchResult == JsValue.Null)
                {
                    return null;
                }
                throw ex;
            }
        }

        protected PatcherOperationScope ApplySingleScript(DocumentsOperationContext context, bool isTestOnly, PatchRequest patch, string collection)
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
                PrepareEngine(patch, scope, jintEngine);

                //scope.PatchObject = scope.ToJsObject(jintEngine, document.Data);
                scope.ActualPatchResult = jintEngine.Invoke("Merge" + collection, scope.PatchObject);

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
          
            RemoveEngineCustomizations(jintEngine, scope);
        }

        private void PrepareEngine(PatchRequest patch, PatcherOperationScope scope, Engine jintEngine)
        {
            int totalScriptSteps = 0;
            
            jintEngine.Global.Delete("LoadDocument", false);
            jintEngine.Global.Delete("IncreaseNumberOfAllowedStepsBy", false);

            CustomizeEngine(jintEngine, scope);

            jintEngine.SetValue("LoadDocument", (Func<string, JsValue>)(key => scope.LoadDocument(key, jintEngine, ref totalScriptSteps)));
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
            jintEngine.Execute(patch.Script, new ParserOptions
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
            engine.Global.Delete("DeleteDocument", false);
            engine.Global.Delete("PutDocument", false);
            engine.Global.Delete("HasTombstone", false);

            engine.SetValue("PutDocument", (Func<string, JsValue, JsValue, JsValue, string>)((key, data, metadata, etag) => scope.PutDocument(key, data, metadata, etag, engine)));
            engine.SetValue("DeleteDocument", (Action<string>)scope.DeleteDocument);
            engine.SetValue("HasTombstone", _hasTombstone);

            var docsArr = engine.Array.Construct(Arguments.Empty);
            var docs = _docs.ToArray();
            for (var i = 0; i < docs.Length; i++)
            {
                var doc = docs[i];
                //TODO : add unit test that has a conflict here to make sure that it is ok
                var jsVal = scope.ToJsObject(engine, doc.Doc, "doc" + i);
                docsArr.FastAddProperty(i.ToString(), jsVal, true, true, true);
            }
            docsArr.FastSetProperty("length", new PropertyDescriptor
            {
                Value = new JsValue(docs.Length),
                Configurable = true,
                Enumerable = true,
                Writable = true,
            });
           
            scope.PatchObject = docsArr;
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

