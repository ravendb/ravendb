using System;
using Jint;
using Raven.Client.Documents.Exceptions.Patching;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;


namespace Raven.Server.Documents.Patch
{
    public class AdminJsConsole : DocumentPatcherBase
    {
        private readonly DocumentDatabase _database;

        public AdminJsConsole(DocumentDatabase database) : base(database)
        {
            _database = database;
        }

        protected override void CustomizeEngine(Engine engine, PatcherOperationScope scope)
        {
        }

        protected override void RemoveEngineCustomizations(Engine engine, PatcherOperationScope scope)
        {
        }

        private const string ExecString = @"function ExecuteAdminScript(databaseInner){{ return (function(database){{ {0} }}).apply(this, [databaseInner]); }};";

        public DynamicJsonValue ApplyScript(AdminJsScript script)
        {
            Engine jintEngine;
            try
            {
                jintEngine = CreateEngine(script.Script, ExecString);
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

            var jsVal = jintEngine.Invoke("ExecuteAdminScript", _database);

            if (jsVal.IsUndefined())
                return null;

            using (var context = DocumentsOperationContext.ShortTermSingleUse(_database))
            using (var scope = new PatcherOperationScope(_database).Initialize(context))
            {
                return scope.ToBlittable(jsVal.AsObject());
            }
        }
    }

    public class AdminJsScript
    {
        public string Script { get; set; }
    }
}