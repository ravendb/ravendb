using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;

namespace Raven.Server.Documents.PeriodicBackup;

public abstract class ResponsibleNodeForBackup
{
    protected ResponsibleNodeForBackup(string nodeTag, string taskName)
    {
        NodeTag = nodeTag;
        TaskName = taskName;
    }

    public string NodeTag { get; }

    public string TaskName { get; }

    public abstract ChosenNodeReason Reason { get; }

    public abstract string ReasonForDecisionLog { get; }

    public abstract bool ShouldLog { get; }

    public enum ChosenNodeReason
    {
        SameResponsibleNode,
        SameResponsibleNodeDueToResourceLimitations,
        SameResponsibleNodeDueToMissingHighlyAvailableTasks,
        MentorNode,
        PinnedMentorNode,
        NonExistingResponsibleNode,
        CurrentResponsibleNodeRemovedFromTopology,
        CurrentResponsibleNodeNotResponding
    }
}

public abstract class ResponsibleNodeForBackupWithLimitedLogging : ResponsibleNodeForBackup
{
    private readonly long _taskId;
    private readonly Dictionary<long, ChosenNodeReason> _lastChosenNodeReasonPerTask;

    protected ResponsibleNodeForBackupWithLimitedLogging(string nodeTag, PeriodicBackupConfiguration configuration, Dictionary<long, ChosenNodeReason> lastChosenNodeReasonPerTask) : base(nodeTag, configuration.Name)
    {
        _taskId = configuration.TaskId;
        _lastChosenNodeReasonPerTask = lastChosenNodeReasonPerTask;
    }

    public override bool ShouldLog
    {
        get
        {
            if (_lastChosenNodeReasonPerTask.TryGetValue(_taskId, out var chosenNodeReason)
                && chosenNodeReason == Reason)
            {
                // we want to log this only once
                return false;
            }

            return true;
        }
    }
}

public class MentorNode : ResponsibleNodeForBackupWithLimitedLogging
{
    public MentorNode(string nodeTag, PeriodicBackupConfiguration configuration, Dictionary<long, ChosenNodeReason> lastChosenNodeReasonPerTask) : base(nodeTag, configuration, lastChosenNodeReasonPerTask)
    {
    }

    public override ChosenNodeReason Reason => ChosenNodeReason.MentorNode;

    public override string ReasonForDecisionLog => $"Node '{NodeTag}' was selected because it's the mentor node for the backup task '{TaskName}'";
}

public class PinnedMentorNode : ResponsibleNodeForBackupWithLimitedLogging
{
    public PinnedMentorNode(string nodeTag, PeriodicBackupConfiguration configuration, Dictionary<long, ChosenNodeReason> lastChosenNodeReasonPerTask) : base(nodeTag, configuration, lastChosenNodeReasonPerTask)
    {
    }

    public override ChosenNodeReason Reason => ChosenNodeReason.PinnedMentorNode;

    public override string ReasonForDecisionLog => $"Node '{NodeTag}' was selected because it's the pinned mentor node for the backup task '{TaskName}'";
}

public class NonExistingResponsibleNode : ResponsibleNodeForBackup
{
    public NonExistingResponsibleNode(string nodeTag, string taskName) : base(nodeTag, taskName)
    {
    }

    public override ChosenNodeReason Reason => ChosenNodeReason.NonExistingResponsibleNode;

    public override string ReasonForDecisionLog => $"No responsible node for backup currently exists for backup task '{TaskName}', setting it to '{NodeTag}'";

    public override bool ShouldLog => true;
}

public class CurrentResponsibleNodeRemovedFromTopology : ResponsibleNodeForBackup
{
    private readonly string _currentResponsibleNode;

    public CurrentResponsibleNodeRemovedFromTopology(string nodeTag, string taskName, string currentResponsibleNode) : base(nodeTag, taskName)
    {
        _currentResponsibleNode = currentResponsibleNode;
    }

    public override ChosenNodeReason Reason => ChosenNodeReason.CurrentResponsibleNodeRemovedFromTopology;

    public override string ReasonForDecisionLog => $"Node '{_currentResponsibleNode}' has been removed from topology. Node '{NodeTag}' will now be responsible for the backup task '{TaskName}'";

    public override bool ShouldLog => true;
}

public class SameResponsibleNode : ResponsibleNodeForBackup
{
    public SameResponsibleNode(string nodeTag, string taskName) : base(nodeTag, taskName)
    {
    }

    public override ChosenNodeReason Reason => ChosenNodeReason.SameResponsibleNode;

    public override string ReasonForDecisionLog => null;

    public override bool ShouldLog => false;
}

public class SameResponsibleNodeDueToResourceLimitations : ResponsibleNodeForBackupWithLimitedLogging
{
    private readonly DatabasePromotionStatus _promotionStatus;

    public SameResponsibleNodeDueToResourceLimitations(string lastResponsibleNode, PeriodicBackupConfiguration configuration, Dictionary<long, ChosenNodeReason> lastChosenNodeReasonPerTask, DatabasePromotionStatus promotionStatus) : base(lastResponsibleNode, configuration, lastChosenNodeReasonPerTask)
    {
        _promotionStatus = promotionStatus;
    }

    public override ChosenNodeReason Reason => ChosenNodeReason.SameResponsibleNodeDueToResourceLimitations;

    public override string ReasonForDecisionLog => $"Node '{NodeTag}' is still responsible for the backup task '{TaskName}' although it's {GetReasonFromPromotionStatus()}";

    private string GetReasonFromPromotionStatus()
    {
        switch (_promotionStatus)
        {
            case DatabasePromotionStatus.OutOfCpuCredits:
                return "out of CPU credits";
            case DatabasePromotionStatus.EarlyOutOfMemory:
                return "in an early out of memory state";
            case DatabasePromotionStatus.HighDirtyMemory:
                return "in high dirty memory state";
            default:
                throw new ArgumentOutOfRangeException($"{nameof(_promotionStatus)}", $"Unhandled case of {_promotionStatus}");
        }
    }
}

public class SameResponsibleNodeDueToMissingHighlyAvailableTasks : ResponsibleNodeForBackupWithLimitedLogging
{
    public SameResponsibleNodeDueToMissingHighlyAvailableTasks(string nodeTag, PeriodicBackupConfiguration configuration, Dictionary<long, ChosenNodeReason> lastChosenNodeReasonPerTask) : base(nodeTag, configuration, lastChosenNodeReasonPerTask)
    {
    }

    public override ChosenNodeReason Reason => ChosenNodeReason.SameResponsibleNodeDueToMissingHighlyAvailableTasks;

    public override string ReasonForDecisionLog => $"Node '{NodeTag}' is still responsible for the backup task '{TaskName}' because the license doesn't include the highly available tasks feature";
}

public class CurrentResponsibleNodeNotResponding : ResponsibleNodeForBackup
{
    private readonly string _currentResponsibleNode;
    private readonly TimeSpan _moveToNewResponsibleNodeGracePeriod;

    public CurrentResponsibleNodeNotResponding(string nodeTag, string taskName, string currentResponsibleNode, TimeSpan moveToNewResponsibleNodeGracePeriod) : base(nodeTag, taskName)
    {
        _currentResponsibleNode = currentResponsibleNode;
        _moveToNewResponsibleNodeGracePeriod = moveToNewResponsibleNodeGracePeriod;
    }

    public override ChosenNodeReason Reason => ChosenNodeReason.CurrentResponsibleNodeNotResponding;

    public override string ReasonForDecisionLog => $"Node '{_currentResponsibleNode}' was in rehab state for {_moveToNewResponsibleNodeGracePeriod}. " +
                                                   $"Node '{NodeTag}' will now be responsible for the backup task '{TaskName}'";

    public override bool ShouldLog => true;
}
