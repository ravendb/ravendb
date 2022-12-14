//-----------------------------------------------------------------------
// <copyright file="AsyncHiLoIdGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Sparrow.Json;

namespace Raven.Client.Documents.Identity
{
    /// <summary>
    /// Generate hilo numbers against a RavenDB document
    /// </summary>
    public class AsyncHiLoIdGenerator
    {
        private readonly DocumentStore _store;
        private readonly string _tag;
        protected string Prefix;
        private long _lastBatchSize;
        private DateTime _lastRangeDate;
        private readonly string _dbName;
        private readonly char _identityPartsSeparator;
        private volatile RangeValue _range;


        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncHiLoIdGenerator"/> class.
        /// </summary>
        public AsyncHiLoIdGenerator(string tag, DocumentStore store, string dbName, char identityPartsSeparator)
        {
            _store = store;
            _tag = tag;
            _dbName = dbName;
            _identityPartsSeparator = identityPartsSeparator;
            _range = new RangeValue(1, 0, null);
        }

        [Obsolete("Will be removed in next major version of the product. Use the GetDocumentIdFromId(NextIdResult) overload")]
        protected virtual string GetDocumentIdFromId(long nextId)
        {
            return $"{Prefix}{nextId}-{ServerTag}";
        }

        protected virtual string GetDocumentIdFromId(NextId result)
        {
            return $"{Prefix}{result.Id}-{result.ServerTag}";
        }

        protected RangeValue Range
        {
            get => _range;
            set => _range = value;
        }

        [DebuggerDisplay("[{Min}-{Max}]: {Current}")]
        protected class RangeValue
        {
            public readonly long Min;
            public readonly long Max;
            public long Current;
            public string ServerTag;

            [Obsolete("Will be removed in next major version of the product. Use RangeValue(min, max, serverTag) instead")]
            public RangeValue(long min, long max)
            {
                Min = min;
                Max = max;
                Current = min - 1;
            }

            public RangeValue(long min, long max, string serverTag)
            {
                Min = min;
                Max = max;
                Current = min - 1;
                ServerTag = serverTag;
            }
        }

        private Lazy<Task> _nextRangeTask = new Lazy<Task>(() => Task.CompletedTask);

        [Obsolete("Use field Range.ServerTag")]
        protected string ServerTag;

        /// <summary>
        /// Generates the document ID.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public async Task<string> GenerateDocumentIdAsync(object entity)
        {
            var nextIdResult = await GetNextIdAsync().ConfigureAwait(false);
            _forTestingPurposes?.BeforeGeneratingDocumentId?.Invoke();
            return GetDocumentIdFromId(nextIdResult);
        }

        public async Task<NextId> GetNextIdAsync()
        {
            while (true)
            {
                var current = _nextRangeTask;

                // local range is not exhausted yet
                var range = Range;
                var id = Interlocked.Increment(ref range.Current);
                if (id <= range.Max)
                {
                    return new NextId
                    {
                        Id = id,
                        ServerTag = range.ServerTag
                    };
                }

                try
                {
                    // let's try to call the existing task for next range
                    await current.Value.ConfigureAwait(false);
                    if (range != Range)
                        continue;
                }
                catch
                {
                    // previous task was faulted, we will try to replace it
                }

                // local range is exhausted , need to get a new range
                var maybeNextTask = new Lazy<Task>(GetNextRangeAsync);
                var nextTask = Interlocked.CompareExchange(ref _nextRangeTask, maybeNextTask, current);
                if (nextTask == current) // replace was successful
                {
                    await maybeNextTask.Value.ConfigureAwait(false);
                    continue;
                }

                try
                {
                    // failed to replace, let's wait on the previous task
                    await nextTask.Value.ConfigureAwait(false);
                }
                catch
                {
                    // previous task was faulted, we will try again
                }
            }
        }

        [Obsolete("Will be removed in next major version of the product. Use GetNextIdAsync instead")]
        public async Task<long> NextIdAsync()
        {
            var result = await GetNextIdAsync().ConfigureAwait(false);
            return result.Id;
        }

        private async Task GetNextRangeAsync()
        {
            var hiloCommand = new NextHiLoCommand(_tag, _lastBatchSize, _lastRangeDate, _identityPartsSeparator, Range.Max);

            var re = _store.GetRequestExecutor(_dbName);
            JsonOperationContext context;
            using (re.ContextPool.AllocateOperationContext(out context))
            {
                await re.ExecuteAsync(hiloCommand, context, sessionInfo: null, token: CancellationToken.None).ConfigureAwait(false);
            }

            Prefix = hiloCommand.Result.Prefix;
            ServerTag = hiloCommand.Result.ServerTag;
            _lastRangeDate = hiloCommand.Result.LastRangeAt;
            _lastBatchSize = hiloCommand.Result.LastSize;
            Range = new RangeValue(hiloCommand.Result.Low, hiloCommand.Result.High, hiloCommand.Result.ServerTag);
        }

        public async Task ReturnUnusedRangeAsync()
        {
            var returnCommand = new HiLoReturnCommand(_tag, Range.Current, Range.Max);

            var re = _store.GetRequestExecutor(_dbName);
            JsonOperationContext context;
            using (re.ContextPool.AllocateOperationContext(out context))
            {
                await re.ExecuteAsync(returnCommand, context, sessionInfo: null, token: CancellationToken.None).ConfigureAwait(false);
            }
        }

        internal TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff();
        }

        internal class TestingStuff
        {
            internal Action BeforeGeneratingDocumentId;
        }

        public class NextId
        {
            public long Id;

            public string ServerTag;
        }
    }
}
