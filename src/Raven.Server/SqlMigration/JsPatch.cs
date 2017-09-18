using System;
using Jint;
using Raven.Server.Config;
using Raven.Server.Documents.Patch;

namespace Raven.Server.SqlMigration
{
    public class JsPatch
    {
        private const string FunctionName = "Execute";
        private const string ExecutionStr = "function " + FunctionName + "(document) {{(function Foo() {{ {0} }}).apply(document)}}";
        private readonly Engine _engine;
        private readonly bool _hasScript;

        public JsPatch(string patchScript, RavenConfiguration config)
        {
            if (string.IsNullOrEmpty(patchScript))
                return;

            _engine = new Engine(options =>
            {
                options.LimitRecursion(64)
                    .SetReferencesResolver(new JintPreventResolvingTasksReferenceResolver())
                    .MaxStatements(config.Patching.MaxStepsForScript)
                    .Strict();
            });

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
                throw new InvalidOperationException($"Error patching document of table '{document.TableName}'", e);
            }
        }
    }
}
