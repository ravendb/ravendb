using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Util.RateLimiting;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.TransactionCommands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron.Global;
using Index = Raven.Server.Documents.Indexes.Index;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Queries
{
    public abstract class AbstractQueryRunner
    {
        public readonly DocumentDatabase Database;

        protected QueryRunner QueryRunner => Database.QueryRunner;

        protected AbstractQueryRunner(DocumentDatabase database)
        {
            Database = database;
        }

        public Index GetIndex(string indexName)
        {
            var index = Database.IndexStore.GetIndex(indexName);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(indexName);

            return index;
        }

        public abstract Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token);

        public abstract Task ExecuteStreamQuery(IndexQueryServerSide query, QueryOperationContext queryContext, HttpResponse response,
            IStreamQueryResultWriter<Document> writer, OperationCancelToken token);

        public abstract Task ExecuteStreamIndexEntriesQuery(IndexQueryServerSide query, QueryOperationContext queryContext, HttpResponse response,
            IStreamQueryResultWriter<BlittableJsonReaderObject> writer, OperationCancelToken token);

        public abstract Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token);

        public abstract Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, QueryOperationContext queryContext, Action<IOperationProgress> onProgress, OperationCancelToken token);

        public abstract Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch,
            BlittableJsonReaderObject patchArgs, QueryOperationContext queryContext, Action<IOperationProgress> onProgress, OperationCancelToken token);

        public abstract Task<SuggestionQueryResult> ExecuteSuggestionQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token);

        protected async Task<SuggestionQueryResult> ExecuteSuggestion(
            IndexQueryServerSide query,
            Index index,
            QueryOperationContext queryContext,
            long? existingResultEtag,
            OperationCancelToken token)
        {
            if (query.Metadata.SelectFields.Length == 0)
                throw new InvalidQueryException("Suggestion query must have at least one suggest token in SELECT.", query.Metadata.QueryText, query.QueryParameters);

            var fields = index.Definition.IndexFields;

            foreach (var f in query.Metadata.SelectFields)
            {
                if (f.IsSuggest == false)
                    throw new InvalidQueryException("Suggestion query must have only suggest tokens in SELECT.", query.Metadata.QueryText, query.QueryParameters);

                var selectField = (SuggestionField)f;

                if (fields.TryGetValue(selectField.Name, out var field) == false)
                    throw new InvalidOperationException($"Index '{index.Name}' does not have a field '{selectField.Name}'.");

                if (field.HasSuggestions == false)
                    throw new InvalidOperationException($"Index '{index.Name}' does not have suggestions configured for field '{selectField.Name}'.");
            }

            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag(queryContext, query.Metadata);
                if (etag == existingResultEtag.Value)
                    return SuggestionQueryResult.NotModifiedResult;
            }

            return await index.SuggestionQuery(query, queryContext, token);
        }

        protected Task<IOperationResult> ExecuteDelete(IndexQueryServerSide query, Index index, QueryOperationOptions options, QueryOperationContext queryContext, Action<DeterminateProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(query, index, options, queryContext, onProgress, (key, retrieveDetails) =>
            {
                var command = new DeleteDocumentCommand(key, null, Database);

                return new BulkOperationCommand<DeleteDocumentCommand>(command, retrieveDetails, x => new BulkOperationResult.DeleteDetails
                {
                    Id = key,
                    Etag = x.DeleteResult?.Etag
                }, null);
            }, token);
        }

        protected Task<IOperationResult> ExecutePatch(IndexQueryServerSide query, Index index, QueryOperationOptions options, PatchRequest patch,
            BlittableJsonReaderObject patchArgs, QueryOperationContext queryContext, Action<DeterminateProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(query, index, options, queryContext, onProgress,
                (key, retrieveDetails) =>
                {
                    var command = new PatchDocumentCommand(queryContext.Documents, key,
                        expectedChangeVector: null,
                        skipPatchIfChangeVectorMismatch: false,
                        patch: (patch, patchArgs),
                        patchIfMissing: (null, null),
                        identityPartsSeparator: Database.IdentityPartsSeparator,
                        debugMode: false,
                        isTest: false,
                        collectResultsNeeded: true,
                        returnDocument: false);

                    return new BulkOperationCommand<PatchDocumentCommand>(command, retrieveDetails,
                        x => new BulkOperationResult.PatchDetails
                        {
                            Id = key,
                            ChangeVector = x.PatchResult.ChangeVector,
                            Status = x.PatchResult.Status
                        },
                        c => c.PatchResult?.Dispose());
                }, token);
        }

        private async Task<IOperationResult> ExecuteOperation<T>(
            IndexQueryServerSide query,
            Index index,
            QueryOperationOptions options,
            QueryOperationContext queryContext,
            Action<DeterminateProgress> onProgress,
            Func<string, bool, BulkOperationCommand<T>> createCommandForId,
            OperationCancelToken token)
            where T : TransactionOperationsMerger.MergedTransactionCommand
        {
            if (index.Type.IsMapReduce())
                throw new InvalidOperationException("Cannot execute bulk operation on Map-Reduce indexes.");

            query = ConvertToOperationQuery(query, options);

            const int batchSize = 1024;

            Queue<string> resultIds;
            try
            {
                var results = await index.Query(query, queryContext, token).ConfigureAwait(false);
                if (options.AllowStale == false && results.IsStale)
                    throw new InvalidOperationException("Cannot perform bulk operation. Index is stale.");

                resultIds = new Queue<string>(results.Results.Count);

                foreach (var document in results.Results)
                {
                    using (document)
                    {
                        token.Delay();

                        resultIds.Enqueue(document.Id.ToString());
                    }
                }
            }
            finally // make sure to close tx if DocumentConflictException is thrown
            {
                queryContext.CloseTransaction();
            }

            var progress = new DeterminateProgress
            {
                Total = resultIds.Count,
                Processed = 0
            };

            onProgress(progress);

            var result = new BulkOperationResult();
            void RetrieveDetails(IBulkOperationDetails details) => result.Details.Add(details);

            using (var rateGate = options.MaxOpsPerSecond.HasValue ? new RateGate(options.MaxOpsPerSecond.Value, TimeSpan.FromSeconds(1)) : null)
            {
                while (resultIds.Count > 0)
                {
                    var command = new ExecuteRateLimitedOperations<string>(resultIds, id =>
                    {
                        var subCommand = createCommandForId(id, options.RetrieveDetails);

                        if (options.RetrieveDetails)
                            subCommand.RetrieveDetails = RetrieveDetails;

                        return subCommand;
                    }, rateGate, token,
                        maxTransactionSize: 16 * Constants.Size.Megabyte,
                        batchSize: batchSize);

                    await Database.TxMerger.Enqueue(command);

                    progress.Processed += command.Processed;

                    onProgress(progress);

                    if (command.NeedWait)
                        rateGate?.WaitToProceed();
                }
            }

            result.Total = progress.Total;
            return result;
        }

        private static IndexQueryServerSide ConvertToOperationQuery(IndexQueryServerSide query, QueryOperationOptions options)
        {
            return new IndexQueryServerSide(query.Metadata)
            {
                Query = query.Query,
                Start = query.Start,
                WaitForNonStaleResultsTimeout = options.StaleTimeout ?? query.WaitForNonStaleResultsTimeout,
                PageSize = query.PageSize,
                QueryParameters = query.QueryParameters,
                DocumentFields = DocumentFields.Id
            };
        }

        internal class BulkOperationCommand<T> : TransactionOperationsMerger.MergedTransactionCommand where T : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly T _command;
            private readonly bool _retrieveDetails;
            private readonly Func<T, IBulkOperationDetails> _getDetails;
            private readonly Action<T> _afterExecuted;

            public BulkOperationCommand(T command, bool retrieveDetails, Func<T, IBulkOperationDetails> getDetails, Action<T> afterExecuted)
            {
                _command = command;
                _retrieveDetails = retrieveDetails;
                _getDetails = getDetails;
                _afterExecuted = afterExecuted;
            }

            public override long Execute(DocumentsOperationContext context, TransactionOperationsMerger.RecordingState recording)
            {
                try
                {
                    var count = _command.Execute(context, recording);

                    if (_retrieveDetails)
                        RetrieveDetails?.Invoke(_getDetails(_command));

                    return count;
                }
                finally
                {
                    _afterExecuted?.Invoke(_command);
                }
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                throw new NotSupportedException($"ToDto() of {nameof(BulkOperationCommand<T>)} Should not be called");
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                throw new NotSupportedException("Should only call Execute() here");
            }

            public Action<IBulkOperationDetails> RetrieveDetails { private get; set; }
        }
    }
}
