using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Util;

namespace Raven.Client.ServerWide.Operations
{
    public class ServerOperationExecutor : IDisposable
    {
        private readonly ConcurrentDictionary<string, ServerOperationExecutor> _cache;

        private readonly string _nodeTag;
        private readonly DocumentStoreBase _store;

        private readonly ClusterRequestExecutor _requestExecutor;
        private readonly ClusterRequestExecutor _initialRequestExecutor;

        public ServerOperationExecutor(DocumentStoreBase store)
            : this(store, CreateRequestExecutor(store), initialRequestExecutor: null, new ConcurrentDictionary<string, ServerOperationExecutor>(StringComparer.OrdinalIgnoreCase), nodeTag: null)
        {
        }

        private ServerOperationExecutor(DocumentStoreBase store, ClusterRequestExecutor requestExecutor, ClusterRequestExecutor initialRequestExecutor, ConcurrentDictionary<string, ServerOperationExecutor> cache, string nodeTag)
        {
            if (store == null)
                throw new ArgumentNullException(nameof(store));
            if (requestExecutor == null)
                throw new ArgumentNullException(nameof(requestExecutor));

            _store = store;
            _requestExecutor = requestExecutor;
            _initialRequestExecutor = initialRequestExecutor;
            _nodeTag = nodeTag;
            _cache = cache;

            store.RegisterEvents(_requestExecutor);

            if (_nodeTag == null)
                store.AfterDispose += (sender, args) => Dispose();
        }

        public void Dispose()
        {
            if (_nodeTag != null)
                return;

            _requestExecutor?.Dispose();

            var cache = _cache;
            if (cache != null)
            {
                foreach (var kvp in cache)
                    kvp.Value._requestExecutor?.Dispose();

                cache.Clear();
            }
        }

        public ServerOperationExecutor ForNode(string nodeTag)
        {
            if (string.IsNullOrWhiteSpace(nodeTag))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(nodeTag));

            if (string.Equals(_nodeTag, nodeTag, StringComparison.OrdinalIgnoreCase))
                return this;

            if (_store.Conventions.DisableTopologyUpdates)
                throw new InvalidOperationException($"Cannot switch server operation executor, because {nameof(Conventions)}.{nameof(_store.Conventions.DisableTopologyUpdates)} is set to 'true'.");

            return _cache.GetOrAdd(nodeTag, tag =>
            {
                var requestExecutor = _initialRequestExecutor ?? _requestExecutor;

                var topology = GetTopology(requestExecutor);

                var node = topology
                    .Nodes
                    .Find(x => string.Equals(x.ClusterTag, tag, StringComparison.OrdinalIgnoreCase));

                if (node == null)
                    throw new InvalidOperationException($"Could not find node '{tag}' in the topology. Available nodes: [{string.Join(", ", topology.Nodes.Select(x => x.ClusterTag))}]");

                var clusterExecutor = ClusterRequestExecutor.CreateForSingleNode(node.Url, _store.Certificate, _store.Conventions);
                return new ServerOperationExecutor(_store, clusterExecutor, requestExecutor, _cache, node.ClusterTag);
            });
        }

        public void Send(IServerOperation operation)
        {
            AsyncHelpers.RunSync(() => SendAsync(operation));
        }

        public TResult Send<TResult>(IServerOperation<TResult> operation)
        {
            return AsyncHelpers.RunSync(() => SendAsync(operation));
        }

        public async Task SendAsync(IServerOperation operation, CancellationToken token = default)
        {
            using (_requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                var command = operation.GetCommand(_requestExecutor.Conventions, context);
                await _requestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token).ConfigureAwait(false);
            }
        }

        public async Task<TResult> SendAsync<TResult>(IServerOperation<TResult> operation, CancellationToken token = default)
        {
            using (_requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                var command = operation.GetCommand(_requestExecutor.Conventions, context);

                await _requestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token).ConfigureAwait(false);
                return command.Result;
            }
        }

        public Operation Send(IServerOperation<OperationIdResult> operation)
        {
            return AsyncHelpers.RunSync(() => SendAsync(operation));
        }

        public async Task<Operation> SendAsync(IServerOperation<OperationIdResult> operation, CancellationToken token = default)
        {
            using (_requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                var command = operation.GetCommand(_requestExecutor.Conventions, context);

                await _requestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token).ConfigureAwait(false);
                return new ServerWideOperation(_requestExecutor, _requestExecutor.Conventions, command.Result.OperationId, command.SelectedNodeTag ?? command.Result.OperationNodeTag);
            }
        }

        private Topology GetTopology(ClusterRequestExecutor requestExecutor)
        {
            Topology topology = null;
            try
            {
                topology = requestExecutor.Topology;
                if (topology == null)
                {
                    // a bit rude way to make sure that topology has been refreshed
                    // but it handles a case when first topology update failed

                    using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
                    {
                        var operation = new GetBuildNumberOperation();
                        var command = operation.GetCommand(requestExecutor.Conventions, context);
                        requestExecutor.Execute(command, context);
                    }

                    topology = requestExecutor.Topology;
                }
            }
            catch
            {
                // ignored
            }

            if (topology == null)
                throw new InvalidOperationException("Could not fetch the topology.");

            return topology;
        }

        private static ClusterRequestExecutor CreateRequestExecutor(DocumentStoreBase store)
        {
            return store.Conventions.DisableTopologyUpdates
                    ? ClusterRequestExecutor.CreateForSingleNode(store.Urls[0], store.Certificate, store.Conventions)
                    : ClusterRequestExecutor.Create(store.Urls, store.Certificate, store.Conventions);
        }
    }
}
