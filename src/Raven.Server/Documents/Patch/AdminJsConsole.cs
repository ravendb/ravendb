using System;
using System.Diagnostics;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Cli;
using Sparrow.Logging;

namespace Raven.Server.Documents.Patch
{
    public sealed class AdminJsConsole
    {
        private readonly DocumentDatabase _database;
        public readonly RavenLogger Log = RavenLogManager.Instance.GetLoggerForServer<AdminJsConsole>();
        private readonly RavenServer _server;

        public AdminJsConsole(RavenServer server, DocumentDatabase database)
        {
            _server = server;
            _database = database;
            if (Log.IsWarnEnabled)
            {
                if (database != null)
                    Log.Warn($"AdminJSConsole : Preparing to execute database script for \"{database.Name}\"");
                else
                    Log.Warn("AdminJSConsole : Preparing to execute server script");

            }
        }

        public string ApplyScript(AdminJsScript script)
        {
            var sw = Stopwatch.StartNew();
            if (Log.IsWarnEnabled)
            {
                Log.Warn($"Script : \"{script.Script}\"");
            }

            try
            {
                DocumentsOperationContext databaseCtx = null;
                using (_server.AdminScripts.GetScriptRunner(new AdminJsScriptKey(script.Script), false, out var run))
                using (_server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverCtx))
                using (_server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext clusterCtx))
                using (_database?.DocumentsStorage.ContextPool.AllocateOperationContext(out databaseCtx))
                using (var result = run.Run(serverCtx, databaseCtx, "execute", new object[] { _server, _database, serverCtx, clusterCtx, databaseCtx }))
                {
                    var toJson = RavenCli.ConvertResultToString(result);

                    if (Log.IsWarnEnabled)
                    {
                        Log.Warn($"Output: {toJson}");
                    }

                    if (Log.IsWarnEnabled)
                    {
                        Log.Warn($"Finished executing database script. Total time: {sw.Elapsed} ");
                    }

                    return toJson;
                }
            }
            catch (Exception e)
            {
                if (Log.IsErrorEnabled)
                {
                    Log.Error("An Exception was thrown while executing the script: ", e);
                }

                throw;
            }
            finally
            {
                _server.AdminScripts.RunIdleOperations();
            }

        }
    }

    public sealed class AdminJsScript
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
    public sealed class AdminJsScriptKey : ScriptRunnerCache.Key
    {
        private readonly string _script;

        public AdminJsScriptKey(string script)
        {
            _script = script;
        }

        public string Script => _script;

        public override void GenerateScript(ScriptRunner runner)
        {
            runner.AddScript($@"function execute(server, database, serverCtx, clusterCtx, databaseCtx){{ 

{_script}

}};");
        }

        private bool Equals(AdminJsScriptKey other)
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
