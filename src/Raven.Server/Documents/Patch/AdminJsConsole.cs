using System;
using System.Diagnostics;
using System.Dynamic;
using Jint;
using Jint.Native;
using Raven.Client.Documents.Exceptions.Patching;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Patch
{
    public class AdminJsConsole : DocumentPatcherBase
    {
        public readonly Logger Log = LoggingSource.Instance.GetLogger<AdminJsConsole>("AdminJsConsole");

        private readonly RavenServer _server;
        private readonly Stopwatch _sw = new Stopwatch();

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
            _sw.Start();
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

            var result =  ConvertResults(jsVal, Database);
            if (Log.IsOperationsEnabled)
            {
                Log.Operations($"Finished executing database script. Total time: {_sw.Elapsed} ");
            }
            _sw.Reset();
            return result;
        }

        public object ApplyServerScript(AdminJsScript script)
        {
            _sw.Start();
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

            var result = ConvertResults(jsVal, Database);
            if (Log.IsOperationsEnabled)
            {
                Log.Operations($"Finished executing server script. Total time: {_sw.Elapsed} ");
            }
            _sw.Reset();
            return result;
        }

        private static object ConvertResults(JsValue jsVal, DocumentDatabase database = null)
        {
            if (jsVal.IsUndefined() || jsVal.IsNull())
                return null;

            if (jsVal.IsObject())
            {
                var obj = jsVal.ToObject();
                if (obj is ExpandoObject == false)
                    return obj;
            }

            using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
            using (var scope = new PatcherOperationScope(database).Initialize(context))
            {
                return scope.ToBlittableValue(jsVal, string.Empty, false);
            }
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