using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents;

public class DatabaseRaftIndexNotifications : AbstractRaftIndexNotifications<RaftIndexNotification>
{

    private readonly RachisLogIndexNotifications _clusterStateMachineLogIndexNotifications;

    public DatabaseRaftIndexNotifications(RachisLogIndexNotifications clusterStateMachineLogIndexNotifications, CancellationToken token) : base(token)
    {
        _clusterStateMachineLogIndexNotifications = clusterStateMachineLogIndexNotifications;
    }

    public override Task<bool> WaitForTaskCompletion(long index, Lazy<Task> waitingTask)
    {
        return _clusterStateMachineLogIndexNotifications.WaitForTaskCompletion(index, waitingTask);
    }

    public override void NotifyListenersAbout(long index, Exception e)
    {
        _clusterStateMachineLogIndexNotifications.NotifyListenersAbout(index, e);

        RecordNotification(new RaftIndexNotification
        {
            Index = index,
            Exception = e
        });

        base.NotifyListenersAbout(index, e);
    }
}
