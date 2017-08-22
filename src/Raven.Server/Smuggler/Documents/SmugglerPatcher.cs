using System;
using Jurassic.Library;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents
{
    public class SmugglerPatcher
    {
        private readonly DatabaseSmugglerOptions _options;
        private readonly DocumentDatabase _database;
        private ScriptRunner.SingleRun _run;

        public SmugglerPatcher(DatabaseSmugglerOptions options, DocumentDatabase database)
        {
            if (string.IsNullOrWhiteSpace(options.TransformScript))
                throw new InvalidOperationException("Cannot create a patcher with empty transform script.");
            _options = options;
            _database = database;

        }

        public Document Transform(Document document, JsonOperationContext context)
        {
            var result = _run.Run(null, "execute", new object[]{document});
            if (result.Value is ObjectInstance == false)
            {
                document.Data.Dispose();
                return null;
            }
            var newDoc = result.Translate<BlittableJsonReaderObject>(context);
            document.Data.Dispose();
            return new Document
            {
                Data = newDoc,
                Id = document.Id,
                Flags = document.Flags,
                NonPersistentFlags = document.NonPersistentFlags
            };
        }

        public IDisposable Initialize()
        {
            var key = new PatchRequest(_options.TransformScript, PatchRequestType.Smuggler);
            var scriptRunner = _database.Scripts.GetScriptRunner(key, out _run);
            _run.ReadOnly = true;
            return scriptRunner;
        }
    }
}
