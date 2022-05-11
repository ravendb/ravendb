using System;
using System.Diagnostics;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;
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
                using (_server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                using (_database?.DocumentsStorage.ContextPool.AllocateOperationContext(out docsCtx))
                {
                    var engineType = _server.AdminScripts.Configuration.JavaScript.EngineType;
                    string toJson;
                    switch (engineType)
                    {
                        case JavaScriptEngineType.Jint:
                            using (_server.AdminScripts.GetScriptRunnerJint(new AdminJsScriptKeyJint(script.Script), readOnly: false, out SingleRunJint run1))
                            using (var result = run1.Run(ctx, docsCtx, "execute", null, new object[] { _server, _database }))
                            {
                                toJson = TypeConverter.ConvertResultToString(result);
                            }
                            break;
                        case JavaScriptEngineType.V8:
                            using (_server.AdminScripts.GetScriptRunnerV8(new AdminJsScriptKeyV8(script.Script), readOnly: false, out SingleRunV8 run2))
                            using (var result = run2.Run(ctx, docsCtx, "execute", null, new object[] { _server, _database }))
                            {
                                toJson = TypeConverter.ConvertResultToString(result);
                            }
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    if (Log.IsOperationsEnabled)
                    {
                        Log.Operations($"Output: {toJson}");
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


    public abstract class AdminJsScriptKey<T> : ScriptRunnerCache.Key
    where T : struct, IJsHandle<T>
    {
        protected readonly string _script;

        protected AdminJsScriptKey(string script)
        {
            _script = script;
        }

        public string Script => _script;

        public override void GenerateScript<T>(ScriptRunner<T> runner)
        {
            runner.AddScript($@"function execute(server, database){{ 

{_script}

}};");
        }
        protected bool Equals(AdminJsScriptKey<T> other)
        {
            return string.Equals(_script, other._script);
        }
        public override int GetHashCode()
        {
            return _script?.GetHashCode() ?? 0;
        }
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((AdminJsScriptKey<T>)obj);
        }
    }

    public class AdminJsScriptKeyJint : AdminJsScriptKey<JsHandleJint>
    {
        public AdminJsScriptKeyJint(string script) : base(script)
        {
        }
    }

    public class AdminJsScriptKeyV8 : AdminJsScriptKey<JsHandleV8>
    {
        public AdminJsScriptKeyV8(string script) : base(script)
        {
        }
    }
}
