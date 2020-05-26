using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Http;
using Sparrow;

namespace Raven.Client.Documents.Session
{
    public class SessionInfo
    {
        [ThreadStatic]
        private static int _clientSessionIdCounter;
        private int _clientSessionId;
        private bool _sessionIdUsed;

        public int SessionId
        {
            get
            {
                _sessionIdUsed = true;
                return _clientSessionId;
            }
        }

        public long? LastClusterTransactionIndex { get; set; }

        public bool AsyncCommandRunning { get; set; }

        public bool NoCaching { get; set; }

        public SessionInfo(string sessionContextKey, int writeBalanceSeed, bool asyncCommandRunning, long? lastClusterTransactionIndex = null, bool noCaching = false)
        {
            SetSessionContext(sessionContextKey, writeBalanceSeed);

            LastClusterTransactionIndex = lastClusterTransactionIndex;
            AsyncCommandRunning = asyncCommandRunning;
            NoCaching = noCaching;
        }

        public void SetSessionContext(string sessionKey, int loadBalancerContextSeed = 0)
        {
            if(_sessionIdUsed)
                throw new InvalidOperationException("Unable to set the session context after it has already been used. The session context can only be modified before it is utliziedn");
            
            if (sessionKey == null)
            {
                _clientSessionId = ++_clientSessionIdCounter;
            }
            else
            {
                _clientSessionId = (int)Hashing.XXHash32.Calculate(sessionKey,
                    (uint)loadBalancerContextSeed);
            }
        }

        internal async Task<ServerNode> GetCurrentSessionNode(RequestExecutor requestExecutor)
        {
            (int Index, ServerNode Node) result;

            switch (requestExecutor.Conventions.LoadBalanceBehavior)
            {
                case LoadBalanceBehavior.UseSessionContext:
                    result = await requestExecutor.GetNodeBySessionId(_clientSessionId).ConfigureAwait(false);
                    return result.Node;
            }
            
            switch (requestExecutor.Conventions.ReadBalanceBehavior)
            {
                case ReadBalanceBehavior.None:
                    result = await requestExecutor.GetPreferredNode().ConfigureAwait(false);
                    break;
                case ReadBalanceBehavior.RoundRobin:
                    result = await requestExecutor.GetNodeBySessionId(_clientSessionId).ConfigureAwait(false);
                    break;
                case ReadBalanceBehavior.FastestNode:
                    result = await requestExecutor.GetFastestNode().ConfigureAwait(false);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(requestExecutor.Conventions.ReadBalanceBehavior.ToString());
            }

            return result.Node;
        }
    }
}
