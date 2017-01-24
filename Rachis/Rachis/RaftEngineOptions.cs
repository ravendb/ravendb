using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Rachis.Communication;
using Rachis.Interfaces;

namespace Rachis
{
    public class RaftEngineOptions
    {
        public const int DefaultElectionTimeout = 1200;
        public const int DefaultHeartbeatTimeout = 300;
        public const int DefaultMaxEntiresPerRequest = 256;
        public RaftEngineOptions(NodeConnectionInfo selfConnectionInfo, ITransportHub transportHub, IRaftStateMachine stateMachine
            , int electionTimeout = DefaultElectionTimeout,int heartbeatTimeout = DefaultHeartbeatTimeout,int maxEntiresPerRequest = DefaultMaxEntiresPerRequest)
        {
            TransportHub = transportHub;
            StateMachine = stateMachine;
            ElectionTimeout = electionTimeout;
            HeartbeatTimeout = heartbeatTimeout;
            SelfConnectionInfo = selfConnectionInfo;
            Name = SelfConnectionInfo.Name;
            MaxEntiresPerRequest = maxEntiresPerRequest;

        }

        public int MaxEntiresPerRequest { get; set; }

        public NodeConnectionInfo SelfConnectionInfo { get; set; }

        public int HeartbeatTimeout { get; set; }

        public int ElectionTimeout { get; set; }

        public string Name { get; set; }
        public ITransportHub TransportHub { get; private set; }
        public IRaftStateMachine StateMachine { get; private set; }
    }
}
