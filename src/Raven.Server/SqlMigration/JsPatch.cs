using System;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.SqlMigration
{
    public class JsPatch
    {
        private const string FunctionName = "execute";
        private readonly bool _hasScript;
        private readonly ScriptRunner.SingleRun _runner;
        private readonly DocumentsOperationContext _context;

        public JsPatch(string patchScript, DocumentsOperationContext context)
        {
            if (string.IsNullOrWhiteSpace(patchScript))
                return;

            _context = context;

            var req = new PatchRequest(patchScript, PatchRequestType.None);
            _context.DocumentDatabase.Scripts.GetScriptRunner(req, true, out _runner);

            _hasScript = true;
        }

        public BlittableJsonReaderObject PatchDocument(BlittableJsonReaderObject document)
        {
            if (_hasScript == false)
                return document;

            try
            {
                return _runner.Run(_context, _context, FunctionName, new object[] { document }).TranslateToObject(_context);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Error patching document", e);
            }
        }
    }
}
