using System;
using System.Diagnostics;
using System.Dynamic;
using Jurassic;
using Jurassic.Library;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Cli;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Patch
{
    public class AdminJsConsole
    {
        public readonly Logger Log = LoggingSource.Instance.GetLogger<AdminJsConsole>("AdminJsConsole");
        private readonly RavenServer _server;
        private Stopwatch _sw;

        public AdminJsConsole(DocumentDatabase database)
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

        private const string ExecutionStr = "function ExecuteAdminScript(databaseInner){{ return (function(database){{ {0} }}).apply(this, [databaseInner]); }};";
        private const string ServerExecutionStr = "function ExecuteAdminScript(serverInner){{ return (function(server){{ {0} }}).apply(this, [serverInner]); }};";

        public object ApplyScript(AdminJsScript script)
        {
            return null;
            //_sw = Stopwatch.StartNew();
            //ScriptEngine scriptEngint;
            //if (Log.IsOperationsEnabled)
            //{
            //    Log.Operations($"Script : \"{script.Script}\"");
            //}
            //try
            //{
            //    scriptEngint = GetEngine(script, ExecutionStr);
            //}
            //catch (Exception e)
            //{
            //    if (Log.IsOperationsEnabled)
            //    {
            //        Log.Operations("An Exception was thrown while preparing the Jint Engine: ", e);
            //    }
            //    throw;
            //}
            //object jsVal;
            //try
            //{
            //    jsVal = scriptEngint.CallGlobalFunction("ExecuteAdminScript", Database);
            //}
            //catch (Exception e)
            //{
            //    if (Log.IsOperationsEnabled)
            //    {
            //        Log.Operations("An Exception was thrown while executing the script: ", e);
            //    }
            //    throw;
            //}

            //if (Log.IsOperationsEnabled)
            //{
            //    Log.Operations($"Finished executing database script. Total time: {_sw.Elapsed} ");
            //}
            //return ConvertResults(jsVal, Database);
        }

        public object ApplyServerScript(AdminJsScript script)
        {
            return null;
            //_sw = Stopwatch.StartNew();
            //ScriptEngine jintEngine;
            //if (Log.IsOperationsEnabled)
            //{
            //    Log.Operations($"Script : \"{script.Script}\"");
            //}
            //try
            //{
            //    jintEngine = GetEngine(script, ServerExecutionStr);
            //}
            //catch (Exception e)
            //{
            //    if (Log.IsOperationsEnabled)
            //    {
            //        Log.Operations("An Exception was thrown while preparing the Jint Engine: ", e);
            //    }
            //    throw;
            //}

            //object jsVal;
            //try
            //{
            //    jsVal = jintEngine.CallGlobalFunction("ExecuteAdminScript", _server);
            //}
            //catch (Exception e)
            //{
            //    if (Log.IsOperationsEnabled)
            //    {
            //        Log.Operations("An Exception was thrown while executing the script: ", e);
            //    }
            //    throw;
            //}

            //if (Log.IsOperationsEnabled)
            //{
            //    Log.Operations($"Finished executing server script. Total time: {_sw.Elapsed} ");
            //}

            //return ConvertResults(jsVal, Database);
        }

        private object ConvertResults(object jsVal, DocumentDatabase database = null)
        {
            if (jsVal == Undefined.Value || jsVal == Null.Value)
                return null;

            if (jsVal is ObjectInstance && (jsVal is ArrayInstance) == false)
            {
                var obj = jsVal as ObjectInstance;
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

        public ScriptEngine GetEngine(AdminJsScript script, string executionString)
        {
            ScriptEngine jintEngine;
            try
            {
                throw new NotImplementedException();

                //jintEngine = CreateEngine(script.Script, executionString);
            }
            catch (NotSupportedException e)
            {
                throw new JavaScriptParseException("Could not parse script", e);
            }
            catch (Jurassic.JavaScriptException e)
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
