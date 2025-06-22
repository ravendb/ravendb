using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Extensions;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.GenAi;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.AI.Enumerators;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Stats;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.ETL.Test;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Json.Sync;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi;

public sealed class GenAiTask : EtlProcess<GenAiItem, GenAiScriptResult, GenAiConfiguration, AiConnectionString,
    GenAiStatsScope, GenAiPerformanceOperation>
{
    public const string GenAiTaskTag = "Gen/AI";

    private const string TestDocumentId = "GenAi/TestDocument";
    private int _maxConcurrency;
    private IChatCompletionClient _chatCompletionClient;

    public GenAiTask(Transformation transformation, GenAiConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
        : base(transformation, configuration, database, serverStore, GenAiTaskTag)
    {
        Metrics = new EtlMetricsCountersManager();
        _maxConcurrency = configuration.MaxConcurrency;

        if (configuration.TestMode == false)
            _chatCompletionClient = GetClient();
    }

    private IChatCompletionClient GetClient()
    {
        if (string.IsNullOrWhiteSpace(Configuration.JsonSchema))
            Configuration.JsonSchema = OllamaChatCompletionClient.GetSchemaFor(Configuration.SampleObject);

        var connectorType = Configuration.Connection.GetActiveProvider();
        IChatCompletionClient client = connectorType switch
        {
            AiConnectorType.Ollama => new OllamaChatCompletionClient(Configuration, Database.ServerStore.ContextPool, IChatCompletionClient.DefaultConventions),
            AiConnectorType.OpenAi => new OpenAiChatCompletionClient(Configuration, Database.ServerStore.ContextPool, IChatCompletionClient.DefaultConventions),
            _ => throw new NotSupportedException($"The specified model (\"{connectorType.ToString()}\") is not supported.")
        };

        return client;
    }

    public override EtlType EtlType => EtlType.GenAi;
    public override bool ShouldTrackCounters() => false;
    public override bool ShouldTrackTimeSeries() => false;

    protected override bool ShouldTrackAttachmentTombstones() => false;

    public override bool ShouldTrackDocumentTombstones() => false;

    protected override IEnumerator<GenAiItem> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection)
    {
        return new DocumentsToGenAiItems(docs, collection);
    }

    protected override IEnumerator<GenAiItem> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection,
        bool trackAttachments)
    {
        throw new NotSupportedException($"{nameof(ConvertTombstonesEnumerator)} is not supported for {nameof(GenAiTask)}");
    }

    protected override IEnumerator<GenAiItem> ConvertAttachmentTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones,
        List<string> collections)
    {
        throw new NotSupportedException($"{nameof(ConvertAttachmentTombstonesEnumerator)} is not supported for {nameof(GenAiTask)}");
    }

    protected override IEnumerator<GenAiItem> ConvertCountersEnumerator(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters,
        string collection)
    {
        throw new NotSupportedException($"{nameof(ConvertCountersEnumerator)} is not supported for {nameof(GenAiTask)}");
    }

    protected override IEnumerator<GenAiItem> ConvertTimeSeriesEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesSegmentEntry> timeSeries,
        string collection)
    {
        throw new NotSupportedException($"{nameof(ConvertTimeSeriesEnumerator)} is not supported for {nameof(GenAiTask)}");
    }

    protected override IEnumerator<GenAiItem> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context,
        IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
    {
        throw new NotSupportedException($"{nameof(ConvertTimeSeriesDeletedRangeEnumerator)} is not supported for {nameof(GenAiTask)}");
    }

    protected override EtlTransformer<GenAiItem, GenAiScriptResult, GenAiStatsScope, GenAiPerformanceOperation>
        GetTransformer(DocumentsOperationContext context)
    {
        return new GenAiScriptTransformer(Database, context, Transformation, null, Configuration);
    }

    protected override string LoadFailureMessage =>
        $"Gen AI task '{Configuration.Name}' failed during model communication or update phase. Retrying in {FallbackTime}";

    protected override void EnterFallbackMode(Exception e, DateTime? lastErrorTime)
    {
        if (e is AggregateException ae &&
            ae.InnerExceptions.OfType<RateLimitException>().FirstOrDefault() is { } rateLimitException)
        {
            FallbackTime = rateLimitException.RetryAfter;
            return;
        }

        base.EnterFallbackMode(e, lastErrorTime);
    }

    protected override bool ExtractionLimitReached(DocumentsOperationContext ctx, GenAiStatsScope stats, GenAiItem currentItem, int batchSize)
    {
        if (stats.NumberOfExtractedItems[EtlItemType.Document] >= Configuration.MaxConcurrency * 8)
        {
            var reason = $"Stopping the batch because it has already processed enough items: {stats.NumberOfExtractedItems[EtlItemType.Document]}";

            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] {reason}");

            stats.RecordBatchTransformationCompleteReason(reason);

            return true;
        }

        return false;
    }

    protected override int LoadInternal(IEnumerable<GenAiScriptResult> items, DocumentsOperationContext context, GenAiStatsScope scope)
    {
        var results = PrepareItemsBeforeSendingToModel(items);
        if (results.Count is 0)
            return 0;

        var exceptions = SendToModel(results, context, scope);

        ApplyUpdateScript(context, results);

        if (exceptions?.Count > 0)
        {
            _maxConcurrency = 1;
            throw new AggregateException(exceptions);
        }

        // we had no errors, re-raise max concurrency slowly
        if (_maxConcurrency < Configuration.MaxConcurrency &&
            // we had sufficient changes to actually use the current limit  
            results.Count >= _maxConcurrency)
        {
            _maxConcurrency++;
        }

        return results.Count;
    }

    private List<Exception> SendToModel(List<GenAiResultItem> items, DocumentsOperationContext context, GenAiStatsScope scope)
    {
        using (var statsScope = scope.For(GenAiOperations.LoadToModel))
        {
            context.CloseTransaction();

            List<Task<(string Result, AiUsage Usage)>> tasks = [];
            Task[] executingTasks = new Task[Math.Max(1, _maxConcurrency)];
            Array.Fill(executingTasks, Task.CompletedTask);
            List<GenAiResultItem> itemsSentToModel = [];

            foreach (var item in items)
            {
                statsScope.NumberOfContextObjects++;

                if (item.ContextOutput.IsCached)
                {
                    statsScope.TotalCachedContexts++;
                    continue; // no change, can skip
                }

                // this is how we ensure that we don't have too many outstanding tasks 
                var idx = Task.WaitAny(executingTasks, CancellationToken);
                statsScope.TotalSentToModel++;

                string json = item.ContextOutput.Context.ToString();
                Task<(string Result, AiUsage Usage)> task;
                try
                {
                    task = _chatCompletionClient.CompleteAsync(Configuration.Prompt, json, CancellationToken);
                }
                catch (Exception e)
                {
                    // if we failed to _start_, we want to handle it in the same manner
                    // and deal with the error in ProcessModelResults
                    task = Task.FromException<(string Result, AiUsage Usage)>(e);
                }

                itemsSentToModel.Add(item);
                tasks.Add(task);
                executingTasks[idx] = task;
            }

            try
            {
                Task.WaitAll(executingTasks, CancellationToken); // only the pending tasks remain here
            }
            catch (Exception)
            {
                // explicitly ignoring this, since we'll handle the error 
                // in ProcessModelResults
            }

            return ProcessModelResults(itemsSentToModel, context, tasks, statsScope);
        }
    }

    private List<Exception> ProcessModelResults(List<GenAiResultItem> items, DocumentsOperationContext context, List<Task<(string Result, AiUsage Usage)>> tasks, GenAiStatsScope statsScope)
    {
        List<Exception> exceptions = null;

        for (int index = 0; index < tasks.Count; index++)
        {
            var task = tasks[index];
            var item = items[index];
            if (task.IsCompletedSuccessfully is false)
            {
                var err = HandleItemError(task, item);
                if (err is null) // can happen for refusal / too many tokens in one item, etc. (already handled) 
                    continue;

                exceptions ??= [];
                exceptions.Add(err);
                continue; // so we won't try to save it 
            }

            (string result, AiUsage usage) = task.Result;

            item.ModelOutput = new ModelOutput
            {
                Output = context.Sync.ReadForMemory(result, item.DocId)
            };

            statsScope.TotalTokensUsed += usage.TotalTokens;
            statsScope.PromptTokensUsed += usage.PromptTokens;
            statsScope.CompletionTokensUsed += usage.CompletionTokens;

            if (Configuration.TestMode)
            {
                item.ModelOutput.Usage = usage;
            }
        }

        return exceptions;

        Exception HandleItemError(Task<(string Result, AiUsage Usage)> task, GenAiResultItem item)
        {
            var singleEx = task.Exception.ExtractSingleInnerException();

            if (Configuration.TestMode)
                throw new InvalidOperationException("Failed to run test", singleEx);

            switch (singleEx)
            {
                // this item cannot be processed, because it has too many items, and retrying isn't going to change that
                case TooManyTokensException:
                // the model refused to answer about this item, and is unlikely to change its mind    
                case RefusedToAnswerException:
                    // in this case, we _intentionally_ want to update the hash so we will _not_ try to update this known bad
                    // item again in the future.
                    item.UpdateHash = true;
                    var msg =
                        $"Model call failed for context in document '{item.DocId}' ({singleEx.GetType().Name}). {Environment.NewLine}" +
                        $"Context was: {item.ContextOutput.Context}{Environment.NewLine}" +
                        $"{singleEx}";

                    Statistics.RecordPartialLoadError(msg, item.DocId);
                    Logger.Log(LogLevel.Warn, msg);
                    return null;
                default:
                    // something bad happened, but this isn't the fault of this item (run out of rate limit, TCP error, etc.)
                    // we will _not_ update the hash in this case, so we *will* reprocess this item the next time
                    item.UpdateHash = false;
                    return singleEx;
            }
        }
    }

    private void ApplyUpdateScript(DocumentsOperationContext context, List<GenAiResultItem> results)
    {
        PatchRequest req = new(Configuration.UpdateScript, PatchRequestType.GenAi);
        var cmd = new GenAiBatchPatchCommand(context, results, req, Configuration.Identifier, Logger, Statistics);

        Database.TxMerger.EnqueueSync(cmd);
    }

    protected override GenAiStatsScope CreateScope(EtlRunStats stats)
    {
        return new GenAiStatsScope(stats);
    }

    protected override string StatsAggregatorTag => "GenAI";

    protected override bool ShouldFilterOutHiLoDocument()
    {
        return true;
    }

    public TestEtlScriptResult RunTest(TestGenAiScript testGenAiScript, DocumentsOperationContext context)
    {
        List<GenAiResultItem> items;
        BlittableJsonReaderObject outputDocument = null;

        Document document = null;

        switch (testGenAiScript.TestStage)
        {
            case TestStage.CreateContextObjects:
            case TestStage.ApplyUpdateScript:
                if (testGenAiScript.Document != null)
                {
                    document = new Document
                    {
                        Data = testGenAiScript.Document,
                        ChangeVector = ChangeVectorUtils.NewChangeVector(context.DocumentDatabase.ServerStore.NodeTag, long.MaxValue, context.DocumentDatabase.DbBase64Id),
                        Id = context.GetLazyString(TestDocumentId)
                    };
                }
                else
                {
                    if (string.IsNullOrEmpty(testGenAiScript.DocumentId))
                        throw new InvalidOperationException("Document or DocumentId must be provided to run GenAI test");

                    context.OpenReadTransaction();
                    document = context.DocumentDatabase.DocumentsStorage.Get(context, testGenAiScript.DocumentId)?.Clone(context);
                    if (document == null)
                        throw new InvalidOperationException($"Document {testGenAiScript.DocumentId} does not exist");
                }
                break;
            case TestStage.SendToModel:
                break;
            default:
                throw new InvalidOperationException("Unknown TestStage type : " + testGenAiScript.TestStage.GetType());
        }

        using var scope = new GenAiStatsScope(new EtlRunStats());
        switch (testGenAiScript.TestStage)
        {
            case TestStage.CreateContextObjects:
                if (context.HasTransaction == false)
                    context.OpenReadTransaction();

                var genAiItem = new GenAiItem(document, Configuration.Collection);
                var transformedResults = Transform([genAiItem], context, scope, new EtlProcessState());
                items = PrepareItemsBeforeSendingToModel(transformedResults);

                context.CloseTransaction();
                break;
            case TestStage.SendToModel:
                _chatCompletionClient ??= GetClient();
                items = testGenAiScript.Input;
                List<Exception> exceptions = SendToModel(items, context, scope);
                if (exceptions is not null)
                    throw new AggregateException(exceptions);

                break;
            case TestStage.ApplyUpdateScript:
                {
                    context.CloseTransaction();
                    using var _ = context.OpenWriteTransaction();

                    items = testGenAiScript.Input;
                    PatchRequest req = new(Configuration.UpdateScript, PatchRequestType.GenAi);
                    PatchDocumentCommand lastPatch = null;
                    var hashes = new List<string>();

                    if (string.IsNullOrEmpty(Configuration.Identifier))
                        Configuration.Identifier = Configuration.GenerateIdentifier();

                    if (testGenAiScript.Document != null)
                    {
                        // the document that was provided as input does not exist (we gave it a dummy id),
                        // so it needs to be written to storage before the patch.
                        // the write-tx is not commited so this won't be persisted.

                        FilterMetadataProperties(context, document);
                        context.DocumentDatabase.DocumentsStorage.Put(context, document!.Id, expectedChangeVector: null, document.Data);
                    }

                    foreach (var item in items)
                    {
                        hashes.Add(item.ContextOutput.AiHash);

                        if (item.ModelOutput is null)
                            continue;

                        var dvj = new DynamicJsonValue
                        {
                            ["output"] = item.ModelOutput.Output,
                            ["input"] = item.ContextOutput.Context
                        };

                        var args = context.ReadObject(dvj, document!.Id);
                        var cmd = lastPatch = new PatchDocumentCommand(
                            context: context,
                            id: document.Id,
                            expectedChangeVector: null,
                            skipPatchIfChangeVectorMismatch: false,
                            patch: (req, args),
                            patchIfMissing: default,
                            createIfMissing: null,
                            identityPartsSeparator: Database.IdentityPartsSeparator,
                            isTest: false,
                            debugMode: false,
                            collectResultsNeeded: true,
                            returnDocument: false,
                            ignoreMaxStepsForScript: false);

                        cmd.Execute(context, recordingState: null);

                        item.DebugActions = cmd.DebugActions;
                        item.DebugOutput = cmd.DebugOutput;
                    }

                    if (lastPatch?.PatchResult?.ModifiedDocument != null)
                        outputDocument = GenAiBatchPatchCommand.UpdateHashesInMetadata(document.Id, lastPatch.PatchResult.ModifiedDocument, Configuration.Identifier, hashes, context);

                    break;
                }

            default:
                throw new InvalidOperationException("Unknown TestStage type : " + testGenAiScript.TestStage.GetType());
        }

        return new GenAiTestScriptResult
        {
            InputDocument = document?.Data,
            Results = items,
            OutputDocument = outputDocument,
            TransformationErrors = Statistics.TransformationErrorsInCurrentBatch.Errors.ToList()
        };
    }

    private static void FilterMetadataProperties(DocumentsOperationContext context, Document document)
    {
        if (document!.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
        {
            metadata.Modifications = new DynamicJsonValue(metadata);

            metadata.Modifications.Remove(Constants.Documents.Metadata.Id);
            metadata.Modifications.Remove(Constants.Documents.Metadata.LastModified);
            metadata.Modifications.Remove(Constants.Documents.Metadata.IndexScore);
            metadata.Modifications.Remove(Constants.Documents.Metadata.ChangeVector);
            metadata.Modifications.Remove(Constants.Documents.Metadata.Flags);

            document.Data.Modifications = new DynamicJsonValue(document.Data)
            {
                [Constants.Documents.Metadata.Key] = metadata
            };
        }
        else if (document.Data.HasParent == false)
            return; // no need to clone

        using (var old = document.Data)
        {
            document.Data = document.Data.Clone(context);
        }
    }

    private static List<GenAiResultItem> PrepareItemsBeforeSendingToModel(IEnumerable<GenAiScriptResult> items)
    {
        // TODO we can do this in the transform phase 

        var results = new List<GenAiResultItem>();

        foreach (var scriptResult in items)
        {
            var item = new GenAiResultItem
            {
                DocId = scriptResult.DocumentId,

                ContextOutput = new ContextOutput
                {
                    Context = scriptResult.Context,
                    IsCached = scriptResult.IsCached,
                    AiHash = scriptResult.AiHash
                }
            };

            results.Add(item);
        }

        return results;
    }

    internal IChatCompletionClient GetChatCompletionClient() => _chatCompletionClient;
}
