using Sparrow.Collections;
using System;
using System.Collections.Generic;

namespace Raven.Client.Connection.Profiling
{
    /// <summary>
    /// Manages all profiling activities for a given item
    /// </summary>
    public class ProfilingContext
    {
        private readonly ConcurrentLruSet<ProfilingInformation> leastRecentlyUsedCache = new ConcurrentLruSet<ProfilingInformation>(NumberOfSessionsToTrack);

        private const int NumberOfSessionsToTrack = 128;

        /// <summary>
        /// Register the action as associated with the sender
        /// </summary>
        public void RecordAction(object sender, RequestResultArgs requestResultArgs)
        {

            var profilingInformationHolder = sender as IHoldProfilingInformation;
            if (profilingInformationHolder == null)
                return;


            profilingInformationHolder.ProfilingInformation.Requests =
                new List<RequestResultArgs>(profilingInformationHolder.ProfilingInformation.Requests)
                {
                    requestResultArgs
                };

            leastRecentlyUsedCache.Push(profilingInformationHolder.ProfilingInformation);
        }

        /// <summary>
        /// Try to get a session matching the specified id.
        /// </summary>
        public ProfilingInformation TryGet(Guid id)
        {
            //TODO: verify that this doesn't break any tests
            //TODO: Maybe add more advanced predicates for when more than one request is sent per session.
            return leastRecentlyUsedCache.LastOrDefault(x => x.Id == id);
        }
    }
}
