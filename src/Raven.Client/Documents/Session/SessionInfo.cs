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

        private int? _sessionId;
        private bool _sessionIdUsed;
        private readonly int _loadBalancerContextSeed;
        private bool _canUseLoadBalanceBehavior;
        private readonly InMemoryDocumentSessionOperations _session;

        public int SessionId
        {
            get
            {
                if (_sessionId == null)
                    SetContextInternal(_session.Conventions.LoadBalancerPerSessionContextSelector?.Invoke(_session.DatabaseName));

                _sessionIdUsed = true;

                return _sessionId.Value;
            }
        }
        
        internal void IncrementRequestCount()
        {
           _session.IncrementRequestCount();
        }

        internal bool CanUseLoadBalanceBehavior => _canUseLoadBalanceBehavior;

        public long? LastClusterTransactionIndex { get; set; }

        public bool AsyncCommandRunning { get; set; }

        public bool NoCaching { get; set; }

        internal string ClusterTransactionId { get; set; }

        internal SessionInfo(InMemoryDocumentSessionOperations session, SessionOptions options, DocumentStoreBase documentStore, bool asyncCommandRunning)
        {
            if (documentStore is null)
                throw new ArgumentNullException(nameof(documentStore));

            _session = session ?? throw new ArgumentNullException(nameof(session));
            _loadBalancerContextSeed = session.RequestExecutor.Conventions.LoadBalancerContextSeed;
            _canUseLoadBalanceBehavior = session.Conventions.LoadBalanceBehavior == LoadBalanceBehavior.UseSessionContext && session.Conventions.LoadBalancerPerSessionContextSelector != null;

            LastClusterTransactionIndex = documentStore.GetLastTransactionIndex(session.DatabaseName);
            AsyncCommandRunning = asyncCommandRunning;
            NoCaching = options.NoCaching;
        }
        
        public void SetContext(string sessionKey)
        {
            if (string.IsNullOrWhiteSpace(sessionKey))
                throw new ArgumentException("Session key cannot be null or whitespace.", nameof(sessionKey));

            SetContextInternal(sessionKey);

            _canUseLoadBalanceBehavior = _canUseLoadBalanceBehavior || _session.Conventions.LoadBalanceBehavior == LoadBalanceBehavior.UseSessionContext;
        }

        private void SetContextInternal(string sessionKey)
        {
            if (_sessionIdUsed)
                throw new InvalidOperationException("Unable to set the session context after it has already been used. The session context can only be modified before it is utilized.");

            if (sessionKey == null)
            {
                _sessionId = ++_clientSessionIdCounter;
            }
            else
            {
                _sessionId = (int)Hashing.XXHash32.Calculate(sessionKey, (uint)_loadBalancerContextSeed);
            }
        }

        internal async Task<ServerNode> GetCurrentSessionNode(RequestExecutor requestExecutor)
        {
            (int Index, ServerNode Node) result;

            switch (requestExecutor.Conventions.LoadBalanceBehavior)
            {
                case LoadBalanceBehavior.UseSessionContext:
                    if (_canUseLoadBalanceBehavior)
                    {
                        result = await requestExecutor.GetNodeBySessionId(SessionId).ConfigureAwait(false);
                        return result.Node;
                    }
                    break;
            }

            switch (requestExecutor.Conventions.ReadBalanceBehavior)
            {
                case ReadBalanceBehavior.None:
                    result = await requestExecutor.GetPreferredNode().ConfigureAwait(false);
                    break;
                case ReadBalanceBehavior.RoundRobin:
                    result = await requestExecutor.GetNodeBySessionId(SessionId).ConfigureAwait(false);
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
