using System;
using System.Diagnostics;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Patch
{
    public class AdminJsConsole
    {
        private readonly DocumentDatabase _database;
        public readonly Logger Log = LoggingSource.Instance.GetLogger<AdminJsConsole>("Server");
        private readonly RavenServer _server;

        public AdminJsConsole(RavenServer server, DocumentDatabase database)
        {
            _server = server;
            _database = database;
            if (Log.IsOperationsEnabled)
            {
                if (database != null)
                    Log.Operations($"AdminJSConsole : Preparing to execute database script for \"{database.Name}\"");
                else
                    Log.Operations("AdminJSConsole : Preparing to execute server script");

            }
        }

        public string ApplyScript(AdminJsScript script)
        {
            var sw = Stopwatch.StartNew();
            if (Log.IsOperationsEnabled)
            {
                Log.Operations($"Script : \"{script.Script}\"");
            }

            try
            {
                DocumentsOperationContext docsCtx = null;
                IJavaScriptOptions jsOptions = _database?.JsOptions ?? _server?.Configuration.JavaScript ?? 
                    (IJavaScriptOptions)(new JavaScriptOptions());
                using (_server.AdminScripts.GetScriptRunner(jsOptions, new AdminJsScriptKey(script.Script), false, out var run))
                using (_server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                using (_database?.DocumentsStorage.ContextPool.AllocateOperationContext(out docsCtx))
                using (var result = run.Run(ctx, docsCtx, "execute", new object[] {_server, _database}))
                {
                    var toJson = TypeConverter.ConvertResultToString(result);

                    if (Log.IsOperationsEnabled)
                    {
                        Log.Operations($"Output: {toJson}");
                    }

                    if (Log.IsOperationsEnabled)
                    {
                        Log.Operations($"Finished executing database script. Total time: {sw.Elapsed} ");
                    }

                    return toJson;
                }
            }
            catch (Exception e)
            {
                if (Log.IsOperationsEnabled)
                {
                    Log.Operations("An Exception was thrown while executing the script: ", e);
                }

                throw;
            }
            finally
            {
                _server.AdminScripts.RunIdleOperations();
            }

        }
    }

    public class AdminJsScript
    {
        public string Script;

        public AdminJsScript(string script)
        {
            Script = script;
        }

        public AdminJsScript()
        {

        }
    }
    public class AdminJsScriptKey : ScriptRunnerCache.Key
    {
        private readonly string _script;

        public AdminJsScriptKey(string script)
        {
            _script = script;
        }

        public string Script => _script;

        public override void GenerateScript(ScriptRunner runner)
        {
            runner.AddScript($@"function execute(server, database){{ 

{_script}

}};");
        }

        protected bool Equals(AdminJsScriptKey other)
        {
            return string.Equals(_script, other._script);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((AdminJsScriptKey)obj);
        }

        public override int GetHashCode()
        {
            return _script?.GetHashCode() ?? 0;
        }
    }
}
