using System;
using Jint;
using Raven.Server.Documents.Patch;

namespace Raven.Server.SqlMigration
{
    public class JsPatch
    {
        private const string FunctionName = "Execute";
        private const string ExecutionStr = "function " + FunctionName + "(document) {{(function () {{ {0} }}).apply(document)}}";
        private readonly Engine _engine;
        private readonly bool _hasScript;

        public JsPatch(string patchScript)
        {
            if (string.IsNullOrEmpty(patchScript))
                return;

            _engine = new Engine(options =>
            {
                options.LimitRecursion(64)
                    .SetReferencesResolver(new ScriptRunner.SingleRun.NullPropgationReferenceResolver())
                    .Strict();
            });

            _engine.Execute(string.Format(ExecutionStr, patchScript));

            _hasScript = true;
        }

        public void PatchDocument(SqlMigrationDocument document)
        {
            if (!_hasScript)
                return;

            try
            {
                _engine.Invoke(FunctionName, document);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Error patching document of table '{document.TableName}'. Document id: {document.Id}", e);
            }
        }
    }
}
