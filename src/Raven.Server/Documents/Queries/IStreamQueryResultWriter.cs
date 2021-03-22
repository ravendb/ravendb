using System;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Queries
{
    public interface IStreamQueryResultWriter<in T> : IAsyncDisposable
    {
        void StartResponse();

        void StartResults();

        void EndResults();

        ValueTask AddResultAsync(T res, CancellationToken token);

        void EndResponse();

        ValueTask WriteErrorAsync(Exception e);

        ValueTask WriteErrorAsync(string error);

        void WriteQueryStatistics(long resultEtag, bool isStale, string indexName, long totalResults, DateTime timestamp);

        bool SupportStatistics { get; }
    }
}
