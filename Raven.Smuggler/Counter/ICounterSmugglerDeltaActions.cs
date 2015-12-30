using System;
using System.Threading;
using System.Threading.Tasks;

using Raven.Database.Counters;

namespace Raven.Smuggler.Counter
{
    public interface ICounterSmugglerDeltaActions : IDisposable
    {
        Task WriteDeltaAsync(CounterState delta, CancellationToken cancellationToken);
    }
}