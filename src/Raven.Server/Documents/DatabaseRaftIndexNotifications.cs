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

    public override async Task<bool> WaitForTaskCompletion(long index, Lazy<Task> waitingTask)
    {
        if (await _clusterStateMachineLogIndexNotifications.WaitForTaskCompletion(index, waitingTask) == false)
            return false;

        foreach (var error in _errors)
        {
            if (error.Index == index)
                error.Exception.Throw(); // rethrow
        }

        return true;
    }

    public override void NotifyListenersAbout(long index, Exception e)
    {
        RecordNotification(new RaftIndexNotification
        {
            Index = index,
            Exception = e
        });

        base.NotifyListenersAbout(index, e);
    }
}
