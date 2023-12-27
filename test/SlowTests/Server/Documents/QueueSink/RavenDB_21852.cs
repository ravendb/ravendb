using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.OngoingTasks;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.QueueSink;

public class RavenDB_21852 : QueueSinkTestBase
{
    public RavenDB_21852(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void CanGetQueueSinkTaskInfo()
    {
        using var store = GetDocumentStore();
        var config = SetupRabbitMqQueueSink(store, "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)",
            new List<string>() {UsersQueueName});

        var op = new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.QueueSink);

        var taskInfo = (OngoingTaskQueueSink)store.Maintenance.Send(op);

        Assert.NotNull(taskInfo);
        Assert.Null(taskInfo.Error);
        Assert.Equal(QueueBrokerType.RabbitMq, taskInfo.BrokerType);

        var nonExisting = new GetOngoingTaskInfoOperation("non-existing", OngoingTaskType.QueueSink);

        var nullTaskInfo = (OngoingTaskQueueSink)store.Maintenance.Send(nonExisting);

        Assert.Null(nullTaskInfo);
    }
}
