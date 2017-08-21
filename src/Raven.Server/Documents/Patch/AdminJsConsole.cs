using System;
using System.Diagnostics;
using System.Dynamic;
using Jint;
using Jint.Native;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Cli;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Patch
{
    public class AdminJsConsole : DocumentPatcherBase
    {
        public readonly Logger Log = LoggingSource.Instance.GetLogger<AdminJsConsole>("AdminJsConsole");
        private readonly RavenServer _server;
        private Stopwatch _sw;

        public AdminJsConsole(DocumentDatabase database) : base(database)
        {
            if (Log.IsOperationsEnabled)
            {
                Log.Operations($"AdminJSConsole : Prepering to execute database script for \"{database.Name}\"");
            }
        }

        public AdminJsConsole(RavenServer server)
        {
            _server = server;
            if (Log.IsOperationsEnabled)
            {
                Log.Operations("AdminJSConsole : Prepering to execute server script");
            }
        }

        protected override void CustomizeEngine(Engine engine, PatcherOperationScope scope)
        {
        }

        protected override void RemoveEngineCustomizations(Engine engine, PatcherOperationScope scope)
        {
        }

        private const string ExecutionStr = "function ExecuteAdminScript(databaseInner){{ return (function(database){{ {0} }}).apply(this, [databaseInner]); }};";
        private const string ServerExecutionStr = "function ExecuteAdminScript(serverInner){{ return (function(server){{ {0} }}).apply(this, [serverInner]); }};";

        public object ApplyScript(AdminJsScript script)
        {
            _sw = Stopwatch.StartNew();
            Engine jintEngine;
            if (Log.IsOperationsEnabled)
            {
                Log.Operations($"Script : \"{script.Script}\"");
            }
            try
            {
                jintEngine = GetEngine(script, ExecutionStr);
            }
            catch (Exception e)
            {
                if (Log.IsOperationsEnabled)
                {
                    Log.Operations("An Exception was thrown while preparing the Jint Engine: ", e);
                }
                throw;
            }
            JsValue jsVal;
            try
            {
                jsVal = jintEngine.Invoke("ExecuteAdminScript", Database);

            }
            catch (Exception e)
            {
                if (Log.IsOperationsEnabled)
                {
                    Log.Operations("An Exception was thrown while executing the script: ", e);
                }
                throw;
            }

            if (Log.IsOperationsEnabled)
            {
                Log.Operations($"Finished executing database script. Total time: {_sw.Elapsed} ");
            }
            return ConvertResults(jsVal, Database);
        }

        public object ApplyServerScript(AdminJsScript script)
        {
            _sw = Stopwatch.StartNew();
            Engine jintEngine;
            if (Log.IsOperationsEnabled)
            {
                Log.Operations($"Script : \"{script.Script}\"");
            }
            try
            {
                jintEngine = GetEngine(script, ServerExecutionStr);
            }
            catch (Exception e)
            {
                if (Log.IsOperationsEnabled)
                {
                    Log.Operations("An Exception was thrown while preparing the Jint Engine: ", e);
                }
                throw;
            }

            JsValue jsVal;
            try
            {
                jsVal = jintEngine.Invoke("ExecuteAdminScript", _server);
            }
            catch (Exception e)
            {
                if (Log.IsOperationsEnabled)
                {
                    Log.Operations("An Exception was thrown while executing the script: ", e);
                }
                throw;
            }

            if (Log.IsOperationsEnabled)
            {
                Log.Operations($"Finished executing server script. Total time: {_sw.Elapsed} ");
            }

            return ConvertResults(jsVal, Database);
        }

        private object ConvertResults(JsValue jsVal, DocumentDatabase database = null)
        {
            if (jsVal.IsUndefined() || jsVal.IsNull())
                return null;

            if (jsVal.IsObject())
            {
                var obj = jsVal.ToObject();
                if (obj is ExpandoObject == false)
                {
                    if (Log.IsOperationsEnabled)
                    {
                        Log.Operations($"Output: {obj}");
                    }
                    return obj;
                }
            }

            object result;
            using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
            using (var scope = new PatcherOperationScope(database).Initialize(context))
            {
                result = scope.ToBlittableValue(jsVal, string.Empty, false);
                if (Log.IsOperationsEnabled)
                {
                    //need to create a clone here because JsonOperationContex.Write() modifies the object 
                    var clone = scope.ToBlittableValue(jsVal, string.Empty, false);
                    Log.Operations($"Output: {RavenCli.ConvertResultToString(context, clone)}");
                }
            }

            return result;
        }

        public Engine GetEngine(AdminJsScript script, string executionString)
        {
            Engine jintEngine;
            try
            {
                jintEngine = CreateEngine(script.Script, executionString);
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
                throw new JavaScriptParseException("Could not parse: " + Environment.NewLine + script.Script, e);
            }
            return jintEngine;
        }
    }

    public class AdminJsScript
    {
        public string Script { get; set; }
    }

    public class AdminJsScriptResult
    {
        public DynamicJsonValue Result { get; set; }
    }
}
