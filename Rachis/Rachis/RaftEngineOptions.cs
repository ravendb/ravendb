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
        public const int DefaultElectionTimeout = 1200*10;
        public const int DefaultHeartbeatTimeout = 300*10;
        public const int DefaultMaxEntiresPerRequest = 256;
        public RaftEngineOptions(NodeConnectionInfo selfConnectionInfo, IRaftStateMachine stateMachine
            , int electionTimeout = DefaultElectionTimeout,int heartbeatTimeout = DefaultHeartbeatTimeout,int maxEntiresPerRequest = DefaultMaxEntiresPerRequest)
        {
            StateMachine = stateMachine;
            ElectionTimeout = electionTimeout;
            HeartbeatTimeout = heartbeatTimeout;
            SelfConnectionInfo = selfConnectionInfo;
            Name = SelfConnectionInfo.Name;
            MaxEntriesPerRequest = maxEntiresPerRequest;

        }

        public int MaxEntriesPerRequest { get; set; }

        public NodeConnectionInfo SelfConnectionInfo { get; set; }

        public int HeartbeatTimeout { get; set; }

        public int ElectionTimeout { get; set; }

        public string Name { get; set; }

        public IRaftStateMachine StateMachine { get; private set; }
        
    }
}
