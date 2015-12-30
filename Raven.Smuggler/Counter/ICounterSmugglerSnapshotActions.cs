using System;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Counters;

namespace Raven.Smuggler.Counter
{
    public interface ICounterSmugglerSnapshotActions : IDisposable
    {
        Task WriteSnapshotAsync(CounterSummary snapshot, CancellationToken cancellationToken);
    }
}