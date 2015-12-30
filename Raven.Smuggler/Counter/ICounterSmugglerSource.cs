using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Counters;
using Raven.Abstractions.Database.Smuggler.Counter;
using Raven.Abstractions.Exceptions;
using Raven.Database.Counters;

namespace Raven.Smuggler.Counter
{
    public interface ICounterSmugglerSource : IDisposable
    {
        void OnException(SmugglerException exception);

        Task InitializeAsync(CounterSmugglerOptions options, CancellationToken cancellationToken);

        bool SupportsMultipleSources { get; }

        IReadOnlyList<ICounterSmugglerSource> Sources { get; }

        bool SupportsRetries { get; }

        bool SupportsPaging { get; }

        Task AfterExecuteAsync(CounterSmugglerOperationState state);

        Task<CounterSmuggleType> GetNextSmuggleTypeAsync(CancellationToken cancellationToken);

        Task SkipDeltasAsync(CancellationToken cancellationToken);

        Task SkipSnapshotsAsync(CancellationToken cancellationToken);

        Task<List<CounterSummary>> ReadSnapshotsAsync(int start, int pageSize, CancellationToken cancellationToken);

        Task<List<CounterState>>  ReadDeltasAsync(int start, int pageSize, CancellationToken cancellationToken);
    }
}