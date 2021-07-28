using System;

using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;
using Raven.Server.Extensions;
using Sparrow;
using Sparrow.Json;
using Sparrow.LowMemory;

using Raven.Client.Exceptions.Documents.Patching;
using V8.Net;

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
            using (document)
            {
                using (_run.ScriptEngine.ChangeMaxStatements(_options.MaxStepsForTransformScript))
                {
                    try
                    {
                        using (ScriptRunnerResult result = _run.Run(context, null, "execute", new object[] { document }))
                        {
                            translatedResult = _run.Translate(result, context, usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                        }
                    }
                    catch (JavaScriptException e)
                    {
                        if (e.InnerException is V8Exception innerException && string.Equals(innerException.Message, "skip", StringComparison.OrdinalIgnoreCase))
                            return null;

                        throw;
                    }
                }

                if (!(translatedResult is BlittableJsonReaderObject bjro))
                    return null;

                var cloned = document.Clone(context);
                using (cloned.Data)
                {
                    cloned.Data = bjro;
                }

                return cloned;
            }
        }

        public IDisposable Initialize()
        {
            var key = new PatchRequest(_options.TransformScript, PatchRequestType.Smuggler);
            return _database.Scripts.GetScriptRunner(key, true, out _run);
        }
    }
}
