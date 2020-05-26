using System;
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
        private readonly int _loadBalancerContextSeed;

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

        public SessionInfo(string sessionKey, int loadBalancerContextSeed, bool asyncCommandRunning, long? lastClusterTransactionIndex = null, bool noCaching = false)
        {
            SetContext(sessionKey, loadBalancerContextSeed);

            _loadBalancerContextSeed = loadBalancerContextSeed;

            LastClusterTransactionIndex = lastClusterTransactionIndex;
            AsyncCommandRunning = asyncCommandRunning;
            NoCaching = noCaching;
        }

        public void SetContext(string sessionKey)
        {
            SetContext(sessionKey, _loadBalancerContextSeed);
        }

        private void SetContext(string sessionKey, int loadBalancerContextSeed)
        {
            if (_sessionIdUsed)
                throw new InvalidOperationException("Unable to set the session context after it has already been used. The session context can only be modified before it is utliziedn");

            if (sessionKey == null)
            {
                _clientSessionId = ++_clientSessionIdCounter;
            }
            else
            {
                _clientSessionId = (int)Hashing.XXHash32.Calculate(sessionKey, (uint)loadBalancerContextSeed);
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
