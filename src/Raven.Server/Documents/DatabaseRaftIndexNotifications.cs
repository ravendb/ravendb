using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Utils;
using static Raven.Server.Documents.DatabasesLandlord;

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
        RecordNotification(new RaftIndexNotification
        {
            Index = index,
            Exception = e
        });

        base.NotifyListenersAbout(index, e);
    }

}
