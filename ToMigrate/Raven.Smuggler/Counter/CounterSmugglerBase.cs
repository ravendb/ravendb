using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.Counter;

namespace Raven.Smuggler.Counter
{
    internal abstract class CounterSmugglerBase
    {
        protected readonly CounterSmugglerNotifications Notifications;

        protected readonly CounterSmugglerOptions Options;

        protected readonly ICounterSmugglerSource Source;

        protected readonly ICounterSmugglerDestination Destination;

        protected CounterSmugglerBase(CounterSmugglerOptions options, CounterSmugglerNotifications notifications, ICounterSmugglerSource source, ICounterSmugglerDestination destination)
        {
            Notifications = notifications;
            Options = options;
            Source = source;
            Destination = destination;
        }

        public abstract Task SmuggleAsync(CounterSmugglerOperationState state, CancellationToken cancellationToken);
    }
}
