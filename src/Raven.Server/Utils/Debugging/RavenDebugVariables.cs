using System;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Utils.Debugging;

public static class RavenDebugVariables
{
    public static class Indexing
    {
        static Indexing()
        {
            string searchEngineToDebug = Environment.GetEnvironmentVariable("RAVEN_DEBUG_INDEXING_SEARCH_ENGINE");

            if (string.IsNullOrEmpty(searchEngineToDebug) == false)
            {
                if (Enum.TryParse<SearchEngineType>(searchEngineToDebug, true, out var engineType)) 
                    SearchEngineToDebug = engineType;
            }

            string batchingFileMarkers = Environment.GetEnvironmentVariable("RAVEN_DEBUG_INDEXING_BATCHING_FILE_MARKERS");

            if (string.IsNullOrEmpty(batchingFileMarkers) == false)
            {
                BatchingFileMarkers = batchingFileMarkers;
            }
        }

        public static SearchEngineType? SearchEngineToDebug { get; private set; }

        public static string BatchingFileMarkers { get; private set; }

        public static bool ShouldPutBatchRunningFileMarker(SearchEngineType searchEngine)
        {
            if (string.IsNullOrEmpty(BatchingFileMarkers))
                return false;

            if (SearchEngineToDebug == null)
            {
                // enable for any index regardless search engine type since it was not set
                return true;
            }

            return searchEngine == SearchEngineToDebug.Value;
        }
    }
}
