using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal sealed class ReplicationHandlerProcessorForProgress : AbstractReplicationHandlerProcessorForProgress<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ReplicationHandlerProcessorForProgress([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                var names = GetNames();
                var performance = GetProcessesProgress(context, names);

                writer.WriteReplicationTaskProgress(context, performance.Values);
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<ReplicationTaskProgress[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

        public IDictionary<long, ReplicationTaskProgress> GetProcessesProgress(DocumentsOperationContext context, StringValues names)
        {
            var replicationTasks = new Dictionary<long, ReplicationTaskProgress>();

            foreach (var handler in RequestHandler.Database.ReplicationLoader.OutgoingHandlers)
            {
                if (handler is not OutgoingPullReplicationHandler && handler is not OutgoingExternalReplicationHandler)
                    continue;

                var node = handler.Destination as ExternalReplicationBase;
                if (node == null)
                    continue;

                var taskId = node.TaskId;

                if (names.Count > 0 && names.Contains(node.Name) == false)
                    continue;

                if (replicationTasks.TryGetValue(taskId, out var taskProgress) == false)
                {
                    replicationTasks[taskId] = new ReplicationTaskProgress
                    {
                        TaskName = node.GetTaskName(),
                        ReplicationType = node.Type,
                        ProcessesProgress = new[]
                        {
                            RequestHandler.Database.ReplicationLoader.GetOutgoingReplicationProgress(context, taskId, handler)
                        }
                    };
                    continue;
                }

                var progressList = taskProgress.ProcessesProgress.ToList();
                progressList.Add(RequestHandler.Database.ReplicationLoader.GetOutgoingReplicationProgress(context, taskId, handler));
                taskProgress.ProcessesProgress = progressList.ToArray();
                replicationTasks[taskId] = taskProgress;
            }

            return replicationTasks;
        }
    }
}
