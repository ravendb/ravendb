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
        protected int _clientSessionId;

        public int SessionId { get;}

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

        public void SetSessionContext(string sessionKey, int writeBalanceSeed = 0)
        {
            if (sessionKey == null)
            {
                _clientSessionId = ++_clientSessionIdCounter;
            }
            else
            {
                _clientSessionId = (int)Hashing.XXHash32.Calculate(sessionKey,
                    (uint)writeBalanceSeed);
            }
        }

        public async Task<ServerNode> GetCurrentSessionNode(RequestExecutor requestExecutor)
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
