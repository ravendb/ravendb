using System;
using Jint;
using Jint.Native;
using Raven.Client.Documents.Exceptions.Patching;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;


namespace Raven.Server.Documents.Patch
{
    public class AdminJsConsole : DocumentPatcherBase
    {
        private readonly RavenServer _server;


        public AdminJsConsole(DocumentDatabase database) : base(database)
        {
        }

        public AdminJsConsole(RavenServer server)
        {
            _server = server;
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
            var jintEngine = GetEngine(script, ExecutionStr);

            var jsVal = jintEngine.Invoke("ExecuteAdminScript", Database);

            return ConvertResults(jsVal, Database);
        }

        public object ApplyServerScript(AdminJsScript script)
        {
            var jintEngine = GetEngine(script, ServerExecutionStr);

            var jsVal = jintEngine.Invoke("ExecuteAdminScript", _server);

            return ConvertResults(jsVal);
        }

        private static object ConvertResults(JsValue jsVal, DocumentDatabase database = null)
        {
            if (jsVal.IsUndefined() || jsVal.IsNull())
                return null;

            if (jsVal.IsObject())
            {
                var obj = jsVal.ToObject();
                var typeName = obj.GetType().Name.ToLowerInvariant();

                if (typeName != "expandoobject")
                    return obj;
            }

            using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
            using (var scope = new PatcherOperationScope(database).Initialize(context))
            {
                return scope.ToBlittableValue(jsVal, string.Empty, false);
            }
        }

        private Engine GetEngine(AdminJsScript script, string executionString)
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