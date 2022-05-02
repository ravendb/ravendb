using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForDeleteOngoingTask<TRequestHandler> : AbstractHandlerProcessorForUpdateDatabaseTask<TRequestHandler>
        where TRequestHandler : RequestHandler
    {
        protected string TaskName;

        private OngoingTaskType _type;
        private long _taskId;
        private DeleteOngoingTaskAction _action;

        protected AbstractOngoingTasksHandlerProcessorForDeleteOngoingTask([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override void OnBeforeResponseWrite(DynamicJsonValue responseJson, object _, long index)
        {
            responseJson[nameof(ModifyOngoingTaskResult.TaskId)] = _taskId;
        }

        protected override async ValueTask OnAfterUpdateConfiguration(TransactionOperationContext context, string databaseName, object _, string raftRequestId)
        {
            if (_type == OngoingTaskType.Subscription)
                await RaiseNotificationForSubscriptionTaskRemoval();

            await _action.Complete($"{raftRequestId}/complete");
        }

        protected abstract ValueTask RaiseNotificationForSubscriptionTaskRemoval();

        protected override async Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, string databaseName, object _, string raftRequestId)
        {
            TaskName = RequestHandler.GetStringQueryString("taskName", required: false);
            _taskId = RequestHandler.GetLongQueryString("id");

            var typeStr = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");
            if (Enum.TryParse(typeStr, true, out  _type) == false)
                throw new ArgumentException($"Unknown task type: {typeStr}", "type");

            _action = new DeleteOngoingTaskAction(this, _taskId, _type, RequestHandler.ServerStore, databaseName, context);

            return await RequestHandler.ServerStore.DeleteOngoingTask(_taskId, TaskName, _type, databaseName, $"{raftRequestId}/delete-ongoing-task");
        }

        private class DeleteOngoingTaskAction
        {
            private readonly ServerStore _serverStore;
            private readonly TransactionOperationContext _context;
            private readonly (string Name, List<string> Transformations) _deletingEtl;
            private readonly string _databaseName;
            private readonly AbstractOngoingTasksHandlerProcessorForDeleteOngoingTask<TRequestHandler> _parent;

            public DeleteOngoingTaskAction(AbstractOngoingTasksHandlerProcessorForDeleteOngoingTask<TRequestHandler> parent, long id, OngoingTaskType type, ServerStore serverStore, string databaseName, TransactionOperationContext context)
            {
                _parent = parent;
                _serverStore = serverStore;
                _context = context;
                _databaseName = databaseName;

                switch (type)
                {
                    case OngoingTaskType.RavenEtl:
                    case OngoingTaskType.SqlEtl:
                        using (context.Transaction == null ? context.OpenReadTransaction() : null)
                        using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, _databaseName))
                        {
                            if (rawRecord == null)
                                break;

                            if (type == OngoingTaskType.RavenEtl)
                            {
                                var ravenEtls = rawRecord.RavenEtls;
                                var ravenEtl = ravenEtls?.Find(x => x.TaskId == id);
                                if (ravenEtl != null)
                                    _deletingEtl = (ravenEtl.Name, ravenEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                            }
                            else
                            {
                                var sqlEtls = rawRecord.SqlEtls;
                                var sqlEtl = sqlEtls?.Find(x => x.TaskId == id);
                                if (sqlEtl != null)
                                    _deletingEtl = (sqlEtl.Name, sqlEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                            }
                        }
                        break;
                }
            }

            public async Task Complete(string raftRequestId)
            {
                if (_deletingEtl.Name != null)
                {
                    foreach (var transformation in _deletingEtl.Transformations)
                    {
                        var (index, _) = await _serverStore.RemoveEtlProcessState(_context, _databaseName, _deletingEtl.Name, transformation,
                            $"{raftRequestId}/{transformation}");

                        await _parent.WaitForIndexNotificationAsync(index);
                    }
                }
            }
        }
    }
}
