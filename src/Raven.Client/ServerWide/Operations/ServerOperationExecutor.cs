using System;
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
        private readonly string _nodeTag;
        private readonly DocumentStoreBase _store;

        private readonly ClusterRequestExecutor _requestExecutor;
        private readonly ClusterRequestExecutor _initialRequestExecutor;

        public ServerOperationExecutor(DocumentStoreBase store)
            : this(store, CreateRequestExecutor(store), initialRequestExecutor: null, nodeTag: null)
        {
        }

        private ServerOperationExecutor(DocumentStoreBase store, ClusterRequestExecutor requestExecutor, ClusterRequestExecutor initialRequestExecutor, string nodeTag)
        {
            if (store == null)
                throw new ArgumentNullException(nameof(store));
            if (requestExecutor == null)
                throw new ArgumentNullException(nameof(requestExecutor));

            _store = store;
            _requestExecutor = requestExecutor;
            _initialRequestExecutor = initialRequestExecutor;
            _nodeTag = nodeTag;

            store.RegisterEvents(_requestExecutor);

            store.AfterDispose += (sender, args) => _requestExecutor.Dispose();
        }

        public void Dispose()
        {
            _requestExecutor.Dispose();
        }

        public ServerOperationExecutor ForNode(string nodeTag)
        {
            if (string.IsNullOrWhiteSpace(nodeTag))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(nodeTag));

            if (string.Equals(_nodeTag, nodeTag, StringComparison.OrdinalIgnoreCase))
                return this;

            if (_store.Conventions.DisableTopologyUpdates)
                throw new InvalidOperationException($"Cannot switch server operation executor, because {nameof(Conventions)}.{nameof(_store.Conventions.DisableTopologyUpdates)} is set to 'true'.");

            var requestExecutor = _initialRequestExecutor ?? _requestExecutor;

            while (true)
            {
                var topology = GetTopology(requestExecutor);

                var node = topology
                    .Nodes
                    .Find(x => string.Equals(x.ClusterTag, nodeTag, StringComparison.OrdinalIgnoreCase));

                if (node == null)
                    throw new InvalidOperationException($"Could not find node '{nodeTag}' in the topology. Available nodes: [{string.Join(", ", topology.Nodes.Select(x => x.ClusterTag))}]");

                var clusterExecutor = ClusterRequestExecutor.CreateForSingleNode(node.Url, _store.Certificate, _store.Conventions);
                return new ServerOperationExecutor(_store, clusterExecutor, requestExecutor, node.ClusterTag);
            }
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
