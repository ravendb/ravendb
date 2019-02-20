using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Utils;
using static Sparrow.Server.Platform.Pal;
using static Sparrow.Server.Platform.PalDefinitions;
using static Sparrow.Server.Platform.PalFlags;

namespace Voron.Impl
{
    public class GlobalPrefetchingBehavior
    {
        private const string PrefetchingThreadName = "Voron Global Prefetching Thread";

        internal static readonly Lazy<GlobalPrefetchingBehavior> GlobalPrefetcher = new Lazy<GlobalPrefetchingBehavior>(() =>
        {
            var prefetcher = new GlobalPrefetchingBehavior();
            var thread = new Thread(prefetcher.VoronPrefetcher)
            {
                IsBackground = true,
                Name = PrefetchingThreadName
            };
            thread.Start();
            return prefetcher;
        });

        private readonly Logger _log = LoggingSource.Instance.GetLogger<GlobalPrefetchingBehavior>("Global Prefetcher");
         
        public readonly SingleConsumerRingBuffer<PrefetchRanges> CommandQueue = new SingleConsumerRingBuffer<PrefetchRanges>(128);

        private unsafe void VoronPrefetcher()
        {
            NativeMemory.EnsureRegistered();

            // We want this to always run, even if we dispose / create new storage env, this is 
            // static for the life time of the process, and environments will register / unregister from
            // it as needed


            // We are already zeroing 512 bytes and with the default behavior can get 16MB prefetched per sys call,
            // so that is quite enough. We expect that we'll only rarely need to do more than 32 at a go, anyway
            const int StackSpace = 32;

            var toPrefetch = stackalloc PrefetchRanges[StackSpace];
            try
            {               
                while (true)
                {
                    try
                    {
                        var commands = CommandQueue.Acquire();
                        if (commands.IsEmpty)
                        {
                            Thread.Sleep(10); // If no command is in the queue, we are surely not busy. 
                            continue;
                        }

                        int prefetchIdx = 0;

                        var memoryToPrefetch = commands.GetEnumerator();
                        while (memoryToPrefetch.MoveNext())
                        {
                            // Prepare the segment information.                   
                            toPrefetch[prefetchIdx] = memoryToPrefetch.Current.Item;

                            prefetchIdx++;
                            if (prefetchIdx >= StackSpace)
                            {
                                // We dont have enough space, so we send the batch to the kernel
                                // we explicitly ignore the return code here, this is optimization only
                                rvn_prefetch_ranges(toPrefetch, StackSpace, out _);
                                prefetchIdx = 0;
                            }
                        }                        

                        if (prefetchIdx != 0)
                        {
                            // we explicitly ignore the return code here, this is optimization only
                            rvn_prefetch_ranges(toPrefetch, prefetchIdx, out _);
                        }
                    }
                    finally
                    {
                        CommandQueue.Release();
                    }                    
                }
            }
            catch (Exception e)
            {
                if (_log.IsOperationsEnabled)
                {
                    _log.Operations("Catastrophic failure in Voron prefetcher ", e);
                }

                // Note that we intentionally don't have error handling here.
                // If this code throws an exception that bubbles up to here, we dont WANT the process
                // to die, since we can degrade gracefully even if the prefetcher thread dies.
            }
            // ReSharper disable once FunctionNeverReturns
        }
    }
}
