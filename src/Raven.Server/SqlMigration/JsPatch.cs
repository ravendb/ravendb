using System;
using Jint;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;

namespace Raven.Server.SqlMigration
{
    public class JsPatch
    {
        private const string FunctionName = "Execute";
        private const string ExecutionStr = "function " + FunctionName + "(document) {{(function Foo() {{ {0} }}).apply(document)}}";
        private readonly Engine _engine;
        private readonly bool _hasScript;

        public JsPatch(string patchScript)
        {
            if (string.IsNullOrEmpty(patchScript))
                return;

            // reaching maximum statements count
            /*var adminJsConsole = new AdminJsConsole(documentDatabase);

            var script = new AdminJsScript
            {
                Script = patchScript
            };

            _engine = adminJsConsole.GetEngine(script, ExecutionStr);*/

            _engine = new Engine();
            _engine.Execute(string.Format(ExecutionStr, patchScript));

            _hasScript = true;
        }

        public void PatchDocument(RavenDocument document)
        {
            if (!_hasScript)
                return;

            try
            {
                _engine.Invoke(FunctionName, document);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Error patching documents of table '{document.TableName}'", e);
            }
        }
    }
}
