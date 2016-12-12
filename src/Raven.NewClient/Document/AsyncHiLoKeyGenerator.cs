//-----------------------------------------------------------------------
// <copyright file="HiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Threading;
using Raven.NewClient.Commands;
using System.Threading.Tasks;
using Sparrow.Json;

namespace Raven.NewClient.Client.Document
{
    /// <summary>
    /// Generate hilo numbers against a RavenDB document
    /// </summary>
    public class AsyncHiLoKeyGenerator : HiLoKeyGeneratorBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="HiLoKeyGenerator"/> class.
        /// </summary>
        public AsyncHiLoKeyGenerator(string tag, DocumentStore store, string dbName, string identityPartsSeparator)
            : base(tag, store, dbName, identityPartsSeparator)
        {
        }

        private Lazy<Task> _nextRangeTask;

        /// <summary>
        /// Generates the document key.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public Task<string> GenerateDocumentKeyAsync(object entity)
        {
            return NextIdAsync().ContinueWith(task => GetDocumentKeyFromId(task.Result));
        }

        public async Task<long> NextIdAsync()
        {
            while (true)
            {
                //local range is not exhausted yet
                var range = Range;
                var id = Interlocked.Increment(ref range.Current);
                if (id <= range.Max)
                    return id;

                //local range is exhausted , need to get a new range
                var maybeNextTask = new Lazy<Task>(GetNextRangeAsync);

                var nextTask = Interlocked.CompareExchange(ref _nextRangeTask,
                                   maybeNextTask, null) ?? maybeNextTask;
                try
                {
                    await nextTask.Value.ConfigureAwait(false);
                }
                finally
                {
                    Interlocked.CompareExchange(ref _nextRangeTask, null, nextTask);
                }
            }
        }

        private async Task GetNextRangeAsync()
        {
            var hiloCommand = new NextHiLoCommand
            {
                Tag = _tag,
                LastBatchSize = _lastBatchSize,
                LastRangeAt = _lastRangeDate,
                IdentityPartsSeparator = _identityPartsSeparator,
                LastRangeMax = Range.Max
            };

            var re = _store.GetRequestExecuter(_dbName);
            JsonOperationContext context;
            using (re.ContextPool.AllocateOperationContext(out context))
            {
                await re.ExecuteAsync(hiloCommand, context).ConfigureAwait(false);
            }

            _prefix = hiloCommand.Result.Prefix;
            _lastRangeDate = hiloCommand.Result.LastRangeAt;
            _lastBatchSize = hiloCommand.Result.LastSize;
            Range = new RangeValue(hiloCommand.Result.Low, hiloCommand.Result.High);
        }

        public async Task ReturnUnusedRangeAsync()
        {
            var returnCommand = new HiLoReturnCommand()
            {
                Tag = _tag,
                End = Range.Max,
                Last = Range.Current
            };

            var re = _store.GetRequestExecuter(_dbName);
            JsonOperationContext context;
            using (re.ContextPool.AllocateOperationContext(out context))
            {
                await re.ExecuteAsync(returnCommand, context).ConfigureAwait(false);
            }
        }
    }
}
