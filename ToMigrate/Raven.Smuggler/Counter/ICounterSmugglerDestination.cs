using System;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.Counter;
using Raven.Abstractions.Exceptions;

namespace Raven.Smuggler.Counter
{
    public interface ICounterSmugglerDestination : IDisposable
    {
        void OnException(SmugglerException exception);

        Task InitializeAsync(CounterSmugglerOptions options, CancellationToken cancellationToken);

        Task AfterExecuteAsync(CounterSmugglerOperationState state);

        ICounterSmugglerDeltaActions DeltaActions();

        ICounterSmugglerSnapshotActions SnapshotActions();

        bool ImportDeltas { get; }

        bool ImportSnapshots { get; }

        bool SupportsOperationState { get; }

        Task<CounterSmugglerOperationState> LoadOperationStateAsync(CounterSmugglerOptions options, CancellationToken cancellationToken);
    }
}