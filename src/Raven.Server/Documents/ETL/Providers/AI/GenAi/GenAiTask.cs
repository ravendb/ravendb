using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Extensions;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.AI.Enumerators;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Stats;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.ETL.Test;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
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
    private ChatCompletionClient _chatCompletionClient;

    public GenAiTask(Transformation transformation, GenAiConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
        : base(transformation, configuration, database, serverStore, GenAiTaskTag)
    {
        Metrics = new EtlMetricsCountersManager();
        _maxConcurrency = configuration.MaxConcurrency;

        if (configuration.TestMode == false)
            _chatCompletionClient = GetClient();
    }
    
    private ChatCompletionClient GetClient() => ChatCompletionClient.CreateChatCompletionClient(Database.DocumentsStorage.ContextPool, Configuration.Connection);

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

    protected override EtlTransformer<GenAiItem, GenAiScriptResult, GenAiStatsScope, GenAiPerformanceOperation> GetTransformer(DocumentsOperationContext context, GenAiStatsScope stats)
    {
        return new GenAiScriptTransformer(Database, context, Transformation, null, Configuration, stats);
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

        List<Exception> exceptions;
        
        // Prevent database unloading during long-running AI operations
        using (Database.PreventFromUnloadingByIdleOperations())
        {
            exceptions = SendToModel(results, context, scope);
        }

        ApplyUpdateScript(context, results, scope);

        if (exceptions?.Count > 0)
        {
            _maxConcurrency = 1;
            throw new AggregateException(exceptions).ExtractSingleInnerException();
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

    private AiAgentConfiguration _agent;

    private AiAgentConfiguration Agent => _agent ??= new AiAgentConfiguration("GenAiAgent", Configuration.ConnectionStringName, Configuration.Prompt)
    {
        OutputSchema = Configuration.JsonSchema,
        SampleObject = Configuration.SampleObject,
        Queries = Configuration.Queries
    };

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

                var handler = new GenAiConversationHandler(Database.ServerStore, Database);
                handler.Initialize(Agent, $"GenAI/{item.DocumentId}", new RequestBody
                {
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = json,
                    Attachments = item.ContextOutput.Attachments,
                }, changeVector: null);

                handler.SetClient(_chatCompletionClient);

                try
                {
                    task = handler.HandleRequest(CancellationToken);
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
                statsScope.ModelCallFailures++;

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
                Output = context.Sync.ReadForMemory(result, item.DocumentId)
            };

            statsScope.Usage ??= new AiUsage();
            statsScope.Usage.CachedTokens += usage.CachedTokens;
            statsScope.Usage.CompletionTokens += usage.CompletionTokens;
            statsScope.Usage.PromptTokens += usage.PromptTokens;
            statsScope.Usage.TotalTokens += usage.TotalTokens;

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
                        $"Model call failed for context in document '{item.DocumentId}' ({singleEx.GetType().Name}). {Environment.NewLine}" +
                        $"Context was: {item.ContextOutput.Context}{Environment.NewLine}" +
                        $"{singleEx}";

                    Statistics.RecordPartialLoadError(msg, item.DocumentId);
                    Logger.Warn(msg);
                    return null;
                default:
                    // something bad happened, but this isn't the fault of this item (run out of rate limit, TCP error, etc.)
                    // we will _not_ update the hash in this case, so we *will* reprocess this item the next time
                    item.UpdateHash = false;
                    return singleEx;
            }
        }
    }

    private void ApplyUpdateScript(DocumentsOperationContext context, List<GenAiResultItem> results, GenAiStatsScope scope)
    {
        PatchRequest req = new(Configuration.UpdateScript, PatchRequestType.GenAi);
        var cmd = new GenAiBatchPatchCommand(context, results, req, Configuration.Identifier, Logger, Statistics, scope);

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
        DynamicJsonValue debugActions = null;
        List<string> debugOutput = null;
        PatchStatus status = PatchStatus.NotModified;
        Document document = null;

        switch (testGenAiScript.TestStage)
        {
            case TestStage.CreateContextObjects:
            case TestStage.ApplyUpdateScript:

                if (testGenAiScript.Document == null && string.IsNullOrEmpty(testGenAiScript.DocumentId))
                    throw new InvalidOperationException("Document or DocumentId must be provided to run GenAI test");

                if (testGenAiScript.Document != null && string.IsNullOrEmpty(testGenAiScript.DocumentId) == false)
                {
                    context.OpenReadTransaction();
                    document = context.DocumentDatabase.DocumentsStorage.Get(context, testGenAiScript.DocumentId, DocumentFields.Id | DocumentFields.LowerId | DocumentFields.ChangeVector);
                    if (document == null)
                        throw new InvalidOperationException($"Document {testGenAiScript.DocumentId} does not exist");

                    document.Data = testGenAiScript.Document;
                }
                else if (testGenAiScript.Document != null)
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
                items = testGenAiScript.Input;
                using (context.OpenReadTransaction())
                    ReloadAttachmentsData(context, items);

                _chatCompletionClient ??= GetClient();
                List<Exception> exceptions = SendToModel(items, context, scope);
                if (exceptions is not null)
                    throw new AggregateException(exceptions);

                // Remove ContextOutputs (as they're unnecessary for the next stage)
                items.ForEach(item => item.ContextOutput.Attachments = null );
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
                            debugMode: true,
                            collectResultsNeeded: true,
                            returnDocument: false,
                            ignoreMaxStepsForScript: false);

                        cmd.Execute(context, recordingState: null);
                    }


                    if (lastPatch != null)
                    {
                        status = lastPatch.PatchResult.Status;
                        debugActions = lastPatch?.DebugActions;
                        debugOutput = lastPatch?.DebugOutput;

                        if (lastPatch?.PatchResult?.ModifiedDocument != null)
                        {
                            outputDocument = GenAiBatchPatchCommand.UpdateHashesInMetadata(document.Id, lastPatch.PatchResult.ModifiedDocument, Configuration.Identifier,
                                hashes, context);
                        }
                    }

                    break;
                }

            default:
                throw new InvalidOperationException("Unknown TestStage type : " + testGenAiScript.TestStage.GetType());
        }
        var originalDoc = document?.Data;
        var modifiedDoc = outputDocument;
        return new GenAiTestScriptResult
        {
            Status = status,
            InputDocument = originalDoc,
            OriginalDocument = originalDoc,
            OutputDocument = modifiedDoc,
            ModifiedDocument = modifiedDoc,
            Results = items,
            DebugActions = debugActions,
            DebugOutput = debugOutput,
            TransformationErrors = Statistics.TransformationErrorsInCurrentBatch.Errors.ToList(),
        };
    }

    private void ReloadAttachmentsData(DocumentsOperationContext context, IEnumerable<GenAiResultItem> items)
    {
        // load the attachments data again and replace the summary(preview) with it
        foreach (var item in items)
        {
            if (item.ContextOutput.Attachments.IsNullOrEmpty())
                continue;

            foreach (var genAtt in item.ContextOutput.Attachments.Where(a => a.Source == AiAttachmentSource.FromAttachment))
            {
                // try to reload again every loaded/not-found attachment
                var attachment = Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, item.DocumentId, genAtt.Name, AttachmentType.Document, changeVector: null);
                if (attachment == null)
                    throw new InvalidOperationException($"The document '{item.DocumentId}' has no attachment with name '{genAtt.Name}' from type '{genAtt.Type}' anymore");
                
                genAtt.Data = GenAiScriptTransformer.GetAttachmentDataAsBase64(attachment, genAtt.Type);
            }
        }
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
                DocumentId = scriptResult.DocumentId,

                ContextOutput = new ContextOutput
                {
                    Context = scriptResult.Context,
                    IsCached = scriptResult.IsCached,
                    AiHash = scriptResult.AiHash,
                    Attachments = scriptResult.Attachments
                }
            };

            results.Add(item);
        }

        return results;
    }

    internal ChatCompletionClient GetChatCompletionClient() => _chatCompletionClient;
}
