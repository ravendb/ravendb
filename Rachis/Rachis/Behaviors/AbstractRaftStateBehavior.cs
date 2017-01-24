using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rachis.Communication;
using Rachis.Messages;
using Sparrow.Collections.LockFree;

namespace Rachis.Behaviors
{
    /// <summary>
    /// This class represent the common behavior of all states
    /// </summary>
    public abstract class AbstractRaftStateBehavior : IDisposable
    {
        protected RaftEngine Engine { get; }

        public abstract RaftEngineState State { get; }

        protected AbstractRaftStateBehavior(RaftEngine engine)
        {
            Engine = engine;
            _random = new Random((int)(engine.Name.GetHashCode() + DateTime.UtcNow.Ticks));
            TimeoutPeriod = _random.Next(engine.Options.ElectionTimeout / 2, engine.Options.ElectionTimeout);
#pragma warning disable 4014
            TimeoutTask(engine.CancellationTokenSource.Token).ContinueWith(t =>
            {
                if(t.IsFaulted)
                    Console.WriteLine(t.Exception); //TODO: log unobserved exceptions                
            });
#pragma warning restore 4014
        }

        public int TimeoutPeriod { get; protected set; }

        public abstract void HandleTimeout();

        public async Task TimeoutTask(CancellationToken cancellationToken)
        {
            while (true)
            {                
                await Task.Delay(TimeoutPeriod, cancellationToken).ConfigureAwait(false);
                if (cancellationToken.IsCancellationRequested)
                    return;
                if (TimeoutEventSlim.IsSet == false)
                {
                    HandleTimeout();
                    return;
                }
                TimeoutEventSlim.Reset();
            }
        }

        public abstract void HandleOnGoingCommunicationFromLeader(ITransportBus transport, CancellationToken ct, AppendEntries ae);

        protected ManualResetEventSlim TimeoutEventSlim = new ManualResetEventSlim();
        private readonly Random _random;

        public virtual void Dispose()
        {
        }

        //For now this method is abstract because i like the idea of separating the logic according to the behavior.
        public abstract void HandleNewConnection(ITransportBus transport, CancellationToken ct);

    }
}
