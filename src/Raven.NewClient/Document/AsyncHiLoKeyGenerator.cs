//-----------------------------------------------------------------------
// <copyright file="HiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Threading;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Commands;
using System.Threading.Tasks;
using Raven.NewClient.Client.Http;
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

        /// <summary>
        /// Generates the document key.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public Task<string> GenerateDocumentKeyAsync(object entity)
        {
            return NextIdAsync().ContinueWith(task => GetDocumentKeyFromId(task.Result));
        }

        ///<summary>
        /// Create the next id (numeric)
        ///</summary>
        public Task<long> NextIdAsync()
        {
            while (true)
            {
                var myRange = Range; // thread safe copy
                var incrementedCurrent = Interlocked.Increment(ref myRange.Current);
                if (incrementedCurrent <= myRange.Max)
                {
                    return CompletedTask.With(incrementedCurrent);
                }

                if (interlockedLock.TryEnter() == false)
                {
                    Interlocked.Increment(ref threadsWaitingForRangeUpdate);
                    mre.WaitOne();
                    Interlocked.Decrement(ref threadsWaitingForRangeUpdate);
                    continue;
                }

                try
                {
                    mre.Reset();

                    if (Range != myRange)
                    {
                        // Lock was contended, and the max has already been changed.
                        mre.Set();
                        interlockedLock.Exit();
                        return NextIdAsync();
                    }

                    return GetNextRangeAsync().ContinueWith(task =>
                    {
                        try
                        {
                            Range = task.Result;
                        }
                        finally
                        {
                            mre.Set();
                            interlockedLock.Exit();
                        }

                        return NextIdAsync();
                    }).Unwrap();
                }
                catch
                {
                    // We only unlock in exceptional cases (and not in a finally clause) because non exceptional cases will either have already
                    // unlocked or will have started a task that will unlock in the future.
                    mre.Set();
                    interlockedLock.Exit();
                    throw;
                }
            }
        }

        private async Task<RangeValue> GetNextRangeAsync( )
        {
            var hiloCommand = new NextHiLoCommand
            {
                Tag = _tag,
                LastBatchSize = _lastBatchSize,
                LastRangeAt = _lastRequestedUtc1,
                IdentityPartsSeparator = _identityPartsSeparator,
                LastRangeMax = Range.Max
            };

            RequestExecuter re = _store.GetRequestExecuter(_dbName);
            JsonOperationContext context;
            using (re.ContextPool.AllocateOperationContext(out context))
            {
                await re.ExecuteAsync(hiloCommand, context).ConfigureAwait(false);
            }

            _prefix = hiloCommand.Result.Prefix;
            _lastRequestedUtc1 = hiloCommand.Result.LastRangeAt;
            _lastBatchSize = hiloCommand.Result.LastSize;
            return new RangeValue(hiloCommand.Result.Low, hiloCommand.Result.High);
        }

        public async Task ReturnUnusedRangeAsync()
        {
            var returnCommand = new HiLoReturnCommand()
            {
                Tag = _tag,
                End = Range.Max,
                Last = Range.Current
            };

            RequestExecuter re = _store.GetRequestExecuter(_dbName);
            JsonOperationContext context;
            using (re.ContextPool.AllocateOperationContext(out context))
            {
                await re.ExecuteAsync(returnCommand, context).ConfigureAwait(false);
            }
        }

    }
}
