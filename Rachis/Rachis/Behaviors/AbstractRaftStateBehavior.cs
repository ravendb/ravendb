using System;
using System.IO;
using Rachis.Messages;
using Newtonsoft.Json;

namespace Rachis.Behaviors
{
    /// <summary>
    ///     This class represent the common behavior of all states
    /// </summary>
    public abstract class AbstractRaftStateBehavior : IDisposable
    {
        protected TimeoutEvent TimeoutEvent;

        public bool AvoidLeadership { get; set; }

        public long CurrentTermWhenWeBecameFollowers { get; set;  }

        protected AbstractRaftStateBehavior(RaftEngine engine)
        {
            Engine = engine;
        }

        protected RaftEngine Engine { get; set; }

        public abstract RaftEngineState State { get; }

        public bool ShouldAccept(AppendEntries a)
        {
            return false;
        }

        public virtual void Dispose()
        {
            TimeoutEvent.Dispose();
        }

        public virtual void HandleTimeout()
        {
            TimeoutEvent.Defer();
            if (Engine.CurrentTopology.IsVoter(Engine.Name) == false)
            {
                //TODO: log timeout, report not been a leader.
                return;
            }
            if (AvoidLeadership && CurrentTermWhenWeBecameFollowers >= Engine.PersistentState.CurrentTerm)
            {
                //TODO:log the fact that we are avoiding leadership since we were leaders and stepped down.
                AvoidLeadership = false;
                return;
            }
            //TODO:Add veto on candidacy?
            //TODO:log the timeout 
            Engine.SetState(RaftEngineState.Candidate);
        }
    }
}