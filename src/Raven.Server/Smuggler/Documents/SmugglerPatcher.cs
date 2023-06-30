using System;
using System.Collections.Generic;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;
using Raven.Server.Extensions;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents
{
    public class SmugglerPatcher
    {
        private readonly DatabaseSmugglerOptions _options;
        private readonly DocumentDatabase _database;
        private ScriptRunner.SingleRun _run;

        private readonly HashSet<string> _skippedDocumentIds = new(StringComparer.OrdinalIgnoreCase);

        public SmugglerPatcher(DatabaseSmugglerOptions options, DocumentDatabase database)
        {
            if (string.IsNullOrWhiteSpace(options.TransformScript))
                throw new InvalidOperationException("Cannot create a patcher with empty transform script.");
            _options = options;
            _database = database;
        }

        public Document Transform(Document document)
        {
            var ctx = document.Data._context;
            using (document)
            {
                object translatedResult;
                using (_run.ScriptEngine.ChangeMaxStatements(_options.MaxStepsForTransformScript))
                {
                    try
                    {
                        using (ScriptRunnerResult result = _run.Run(ctx, null, "execute", new object[] { document }))
                        {
                            translatedResult = _run.Translate(result, ctx, usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                        }
                    }
                    catch (Client.Exceptions.Documents.Patching.JavaScriptException e)
                    {
                        if (e.InnerException is Jint.Runtime.JavaScriptException innerException && string.Equals(innerException.Message, "skip", StringComparison.OrdinalIgnoreCase))
                        {
                            _skippedDocumentIds.Add(document.Id);
                            return null;
                        }

                        throw;
                    }
                }

                if (translatedResult is Document d)
                    return d.Clone(ctx);

                if (!(translatedResult is BlittableJsonReaderObject bjro))
                    return null;

                if (ctx.CachedProperties.NeedClearPropertiesCache())
                {
                    ctx.CachedProperties.ClearRenew();
                }

                return document.CloneWith(ctx, bjro);
            }
        }

        public bool ShouldSkip(LazyStringValue documentId)
        {
            return _skippedDocumentIds.Count > 0 && _skippedDocumentIds.Contains(documentId);
        }

        public IDisposable Initialize()
        {
            _skippedDocumentIds.Clear();

            var key = new PatchRequest(_options.TransformScript, PatchRequestType.Smuggler);
            return _database.Scripts.GetScriptRunner(key, true, out _run);
        }
    }
}
