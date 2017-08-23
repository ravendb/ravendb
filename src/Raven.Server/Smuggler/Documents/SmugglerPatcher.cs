using System;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;
using Raven.Server.Smuggler.Documents.Data;
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
			object translatedResult;
            using (var result = _run.Run(null, "execute", new object[] {document}))
            try
            {
                translatedResult = _run.Translate(result, context, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            }
            finally
            {
                document.Data.Dispose();
            }

            if (translatedResult is  BlittableJsonReaderObject == false)
                return null;
            
            return new Document
            {
                Data = (BlittableJsonReaderObject)translatedResult,
                Id = document.Id,
                Flags = document.Flags,
                NonPersistentFlags = document.NonPersistentFlags
            };
        }

        public IDisposable Initialize()
        {
            var key = new PatchRequest(_options.TransformScript, PatchRequestType.Smuggler);
            return _database.Scripts.GetScriptRunner(key, true, out _run);
        }
    }
}
