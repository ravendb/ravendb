using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForDeleteOngoingTask<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseTask<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected string TaskName;

        private OngoingTaskType _type;
        private long _taskId;
        private DeleteOngoingTaskAction _action;

        protected AbstractOngoingTasksHandlerProcessorForDeleteOngoingTask([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void OnBeforeResponseWrite(TransactionOperationContext context, DynamicJsonValue responseJson, object _, long index)
        {
            responseJson[nameof(ModifyOngoingTaskResult.TaskId)] = _taskId;
        }

        protected override async ValueTask OnAfterUpdateConfiguration(TransactionOperationContext context, object _, string raftRequestId)
        {
            if (_type == OngoingTaskType.Subscription)
                await RaiseNotificationForSubscriptionTaskRemoval();

            await _action.Complete($"{raftRequestId}/complete");

            var description = $"delete-{_type}";
            description += $"{(string.IsNullOrEmpty(TaskName) ? string.Empty : $" with task name: '{TaskName}'")}";
            RequestHandler.LogTaskToAudit(description, _taskId, configuration: null);
        }

        protected abstract ValueTask RaiseNotificationForSubscriptionTaskRemoval();

        protected override async Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, object _, string raftRequestId)
        {
            TaskName = RequestHandler.GetStringQueryString("taskName", required: false);
            _taskId = RequestHandler.GetLongQueryString("id");

            var typeStr = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");
            if (Enum.TryParse(typeStr, true, out  _type) == false)
                throw new ArgumentException($"Unknown task type: {typeStr}", "type");

            _action = new DeleteOngoingTaskAction(RequestHandler, _taskId, _type, RequestHandler.ServerStore, context);

            return await RequestHandler.ServerStore.DeleteOngoingTask(_taskId, TaskName, _type, RequestHandler.DatabaseName, $"{raftRequestId}/delete-ongoing-task");
        }

        private sealed class DeleteOngoingTaskAction
        {
            private readonly ServerStore _serverStore;
            private readonly TransactionOperationContext _context;
            private readonly (string Name, List<string> Transformations) _deletingEtl;
            private readonly (string Name, List<string> Scripts) _deletingQueueSink;
            private readonly TRequestHandler _requestHandler;

            public DeleteOngoingTaskAction(TRequestHandler requestHandler, long id, OngoingTaskType type, ServerStore serverStore, TransactionOperationContext context)
            {
                _requestHandler = requestHandler;
                _serverStore = serverStore;
                _context = context;

                using (context.Transaction == null ? context.OpenReadTransaction() : null)
                using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, _requestHandler.DatabaseName))
                {
                    if (rawRecord == null)
                        return;

                    switch (type)
                    {
                        case OngoingTaskType.RavenEtl:
                            var ravenEtls = rawRecord.RavenEtls;
                            var ravenEtl = ravenEtls?.Find(x => x.TaskId == id);
                            if (ravenEtl != null)
                                _deletingEtl = (ravenEtl.Name, ravenEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                            break;
                        case OngoingTaskType.SqlEtl:
                            var sqlEtls = rawRecord.SqlEtls;
                            var sqlEtl = sqlEtls?.Find(x => x.TaskId == id);
                            if (sqlEtl != null)
                                _deletingEtl = (sqlEtl.Name, sqlEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                            break;
                        case OngoingTaskType.OlapEtl:
                            var olapEtls = rawRecord.OlapEtls;
                            var olapEtl = olapEtls?.Find(x => x.TaskId == id);
                            if (olapEtl != null)
                                _deletingEtl = (olapEtl.Name, olapEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                            break;
                        case OngoingTaskType.ElasticSearchEtl:
                            var elasticEtls = rawRecord.ElasticSearchEtls;
                            var elasticEtl = elasticEtls?.Find(x => x.TaskId == id);
                            if (elasticEtl != null)
                                _deletingEtl = (elasticEtl.Name, elasticEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                            break;
                        case OngoingTaskType.QueueSink:
                            var queueSinks = rawRecord.QueueSinks;
                            var queueSink = queueSinks?.Find(x => x.TaskId == id);
                            if (queueSink != null)
                                _deletingQueueSink = (queueSink.Name, queueSink.Scripts.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                            break;
                        case OngoingTaskType.SnowflakeEtl:
                            var snowflakeEtls = rawRecord.SnowflakeEtls;
                            var snowflakeEtl = snowflakeEtls?.Find(x => x.TaskId == id);
                            if (snowflakeEtl != null)
                                _deletingEtl = (snowflakeEtl.Name, snowflakeEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                            break;
                    }
                }
            }

            public async Task Complete(string raftRequestId)
            {
                long? maxIndex = null;

                if (_deletingEtl.Name != null)
                {
                    foreach (var transformation in _deletingEtl.Transformations)
                    {
                        var (index, _) = await _serverStore.RemoveEtlProcessState(_context, _requestHandler.DatabaseName, _deletingEtl.Name, transformation,
                            $"{raftRequestId}/{transformation}");

                        maxIndex = maxIndex.HasValue == false ? index : Math.Max(maxIndex.Value, index);
                    }
                }

                if (_deletingQueueSink.Name != null)
                {

                    foreach (var script in _deletingQueueSink.Scripts)
                    {
                        var (index, _) = await _serverStore.RemoveQueueSinkProcessState(_context, _requestHandler.DatabaseName, _deletingQueueSink.Name, script,
                            $"{raftRequestId}/{script}");

                        maxIndex = maxIndex.HasValue == false ? index : Math.Max(maxIndex.Value, index);
                    }
                }

                if (maxIndex.HasValue)
                    await _requestHandler.WaitForIndexNotificationAsync(maxIndex.Value);
            }
        }
    }
}
