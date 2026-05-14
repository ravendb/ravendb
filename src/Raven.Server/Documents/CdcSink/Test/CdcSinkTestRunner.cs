using System;
using System.Collections.Generic;
using System.Linq;
using Jint;
using Jint.Native;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Test;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.CdcSink.Test
{
    /// <summary>
    /// Drives the CDC test-mapping endpoint: takes raw source rows (already fetched from
    /// the source DB by the migrator's <c>FetchRowsAsync</c>), pushes each row through the
    /// CDC sink's column mapping + patch dispatcher, and collects the resulting documents.
    /// One JS run per row so a script that throws on row N still leaves rows 0..N-1 and
    /// N+1..end populated in the response.
    /// </summary>
    public static class CdcSinkTestRunner
    {
        public static TestCdcSinkMappingResult Run(
            DocumentDatabase database,
            DocumentsOperationContext context,
            CdcSinkConfiguration configuration,
            CdcSinkTableConfig targetTable,
            string[] sourceColumnNames,
            List<object[]> rows,
            TestCdcSinkOperation operation)
        {
            var result = new TestCdcSinkMappingResult();

            configuration.TestMode = true;
            // CdcSinkConfiguration.Validate(validateConnection: true) requires Initialize() first,
            // which captures the SqlConnectionString. Test mode doesn't run the actual sink, so the
            // connection isn't part of the validated state — skip the connection check.
            if (configuration.Validate(out var validationErrors, validateConnection: false) == false)
            {
                foreach (var error in validationErrors)
                    result.Errors.Add(error);
                return result;
            }

            var docProcessor = new CdcSinkDocumentProcessor(configuration, defaultSchema: "");
            var tableSchema = targetTable.SourceTableSchema ?? string.Empty;
            var processor = docProcessor.GetProcessor(tableSchema, targetTable.SourceTableName);
            if (processor == null)
            {
                result.Errors.Add($"Target table '{tableSchema}.{targetTable.SourceTableName}' is not registered in the document processor.");
                return result;
            }

            try
            {
                processor.SetSourceColumnNames(sourceColumnNames);
            }
            catch (Exception e)
            {
                result.Errors.Add($"Source column layout doesn't match the configured columns: {e.Message}");
                return result;
            }

            var isDelete = operation == TestCdcSinkOperation.Delete;
            var ignoreDeletes = isDelete && (targetTable.OnDelete?.IgnoreDeletes ?? false);
            var wouldDelete = isDelete && ignoreDeletes == false;

            // For embedded/linked tables we'd need to simulate the parent doc state; for MVP we only
            // support root-table tests and surface the limitation so Studio can show a hint. It's
            // advisory rather than a hard error — the root-table results below are still valid —
            // so route it through Warnings, not Errors.
            if (targetTable.LinkedTables is { Count: > 0 } || targetTable.EmbeddedTables is { Count: > 0 })
            {
                result.Warnings.Add("Linked and embedded tables configured on this table are not exercised in test mode. " +
                                    "Only the root row mapping (and its patch / OnDelete patch) runs.");
            }

            var patchRequest = docProcessor.CombinedPatchRequest;
            var scriptKey = isDelete ? processor.KeyOnDelete : processor.Key;
            var hasUserScript = patchRequest != null && HasScriptForKey(configuration, targetTable, isDelete);

            ScriptRunner.ReturnRun returnRun = default;
            ScriptRunner.SingleRun runner = null;
            try
            {
                if (hasUserScript)
                {
                    returnRun = database.Scripts.GetScriptRunner(patchRequest, readOnly: false, out runner);
                    runner.DebugMode = true;
                    runner.DebugOutput ??= new List<string>();
                }

                // Read transaction is sufficient — the runner mints synthetic Blittables via
                // context.ReadObject + runner.Translate but never writes them back to storage.
                // No call to OpenWriteTransaction means we don't contend with concurrent writers
                // on the same database for what is effectively a preview / dry-run.
                using (context.OpenReadTransaction())
                {
                    foreach (var values in rows)
                    {
                        result.Results.Add(BuildRowResult(
                            database, context, processor, scriptKey, values,
                            hasUserScript, runner,
                            isDelete, wouldDelete, ignoreDeletes));
                    }
                }
            }
            finally
            {
                returnRun.Dispose();
            }

            return result;
        }

        private static bool HasScriptForKey(CdcSinkConfiguration configuration, CdcSinkTableConfig targetTable, bool isDelete)
        {
            if (isDelete)
                return string.IsNullOrEmpty(targetTable.OnDelete?.Patch) == false;
            return string.IsNullOrEmpty(targetTable.Patch) == false;
        }

        private static TestCdcSinkRowResult BuildRowResult(
            DocumentDatabase database,
            DocumentsOperationContext context,
            CdcSinkTableProcessor processor,
            string scriptKey,
            object[] values,
            bool hasUserScript,
            ScriptRunner.SingleRun runner,
            bool isDelete,
            bool wouldDelete,
            bool ignoreDeletes)
        {
            var rowResult = new TestCdcSinkRowResult
            {
                WouldDelete = wouldDelete,
                IgnoreDeletes = ignoreDeletes,
            };

            try
            {
                using var sourceRowDoc = BuildSourceRowJson(context, processor.SourceColumnNames, values);
                rowResult.SourceRow = sourceRowDoc.ToString();

                var mapped = processor.MapColumns(values, context);
                rowResult.DocumentId = processor.GenerateDocumentId(values);

                mapped[Raven.Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Raven.Client.Constants.Documents.Metadata.Collection] = processor.CollectionName,
                };

                using var mappedDoc = context.ReadObject(mapped, "cdc-test-mapped-" + rowResult.DocumentId);
                rowResult.Document = mappedDoc.ToString();

                if (hasUserScript)
                {
                    rowResult.DebugOutput = RunPatchAndCollectDebug(
                        context, runner, processor, scriptKey, values,
                        rowResult, mappedDoc);
                }
            }
            catch (Exception e)
            {
                rowResult.Error = e.Message;
            }

            return rowResult;
        }

        private static List<string> RunPatchAndCollectDebug(
            DocumentsOperationContext context,
            ScriptRunner.SingleRun runner,
            CdcSinkTableProcessor processor,
            string scriptKey,
            object[] values,
            TestCdcSinkRowResult rowResult,
            BlittableJsonReaderObject mappedDoc)
        {
            runner.DebugOutput.Clear();

            var documentId = rowResult.DocumentId;
            var docInstance = (BlittableObjectInstance)runner.Translate(context,
                new Document { Data = mappedDoc, Id = context.GetLazyString(documentId) }).AsObject();

            var rowObj = new JsObject(runner.ScriptEngine);
            rowObj.FastSetDataProperty("table", scriptKey);
            rowObj.FastSetDataProperty("row", BuildRowJsObject(runner.ScriptEngine, processor.SourceColumnNames, values));
            rowObj.FastSetDataProperty("old", JsValue.Null);
            var rowsArray = new JsArray(runner.ScriptEngine, [rowObj]);

            try
            {
                using var scriptResult = runner.Run(context, context, "execute", documentId,
                    [docInstance, rowsArray]);

                // Surface the patched document so Studio sees the post-script state instead of
                // the pre-patch mapping. If the script explicitly called del()/put() we leave
                // the response as-is (mappedDoc) since the script chose to deviate.
                if (runner.OriginalDocumentUpdatedOrDeleted == false)
                {
                    using var patchedDoc = scriptResult.TranslateToObject(context,
                        usageMode: BlittableJsonDocumentBuilder.UsageMode.None);
                    rowResult.Document = patchedDoc.ToString();
                }
            }
            catch (Exception e)
            {
                rowResult.Error = e.Message;
            }

            return new List<string>(runner.DebugOutput);
        }

        private static JsObject BuildRowJsObject(Jint.Engine engine, string[] names, object[] values)
        {
            var o = new JsObject(engine);
            for (int i = 0; i < names.Length && i < values.Length; i++)
            {
                o.FastSetDataProperty(names[i], values[i] switch
                {
                    null => JsValue.Null,
                    byte[] bytes => Convert.ToBase64String(bytes),
                    Guid guid => guid.ToString(),
                    _ => JsValue.FromObject(engine, values[i])
                });
            }
            return o;
        }

        private static BlittableJsonReaderObject BuildSourceRowJson(
            DocumentsOperationContext context,
            string[] columnNames,
            object[] values)
        {
            var dynamic = new DynamicJsonValue();
            for (int i = 0; i < columnNames.Length && i < values.Length; i++)
            {
                dynamic[columnNames[i]] = NormalizeForJson(values[i]);
            }
            return context.ReadObject(dynamic, "cdc-test-source-row");
        }

        private static object NormalizeForJson(object value)
        {
            return value switch
            {
                null => null,
                byte[] bytes => Convert.ToBase64String(bytes),
                Guid guid => guid.ToString(),
                bool or int or long or float or double or decimal
                    or DateTime or DateOnly or DateTimeOffset or string => value,
                _ => value.ToString()
            };
        }
    }
}
