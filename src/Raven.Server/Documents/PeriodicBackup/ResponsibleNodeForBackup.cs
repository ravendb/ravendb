using System;

namespace Raven.Server.Documents.PeriodicBackup;

public abstract class ResponsibleNodeForBackup
{
    protected ResponsibleNodeForBackup(string nodeTag)
    {
        NodeTag = nodeTag;
    }

    public string NodeTag { get; }

    public abstract ChosenNodeReason Reason { get; }

    public abstract string ReasonForDecisionLog { get; }

    public enum ChosenNodeReason
    {
        MentorNode,
        PinnedMentorNode,
        NonExistingResponsibleNode,
        CurrentResponsibleNodeRemovedFromTopology,
        UnchangedResponsibleNode,
        CurrentResponsibleNodeNotResponding
    }
}

public class MentorNode : ResponsibleNodeForBackup
{
    public MentorNode(string nodeTag) : base(nodeTag)
    {
    }

    public override ChosenNodeReason Reason => ChosenNodeReason.MentorNode;

    public override string ReasonForDecisionLog => $"Node '{NodeTag}' was selected because it's the mentor node for this backup task";
}

public class PinnedMentorNode : ResponsibleNodeForBackup
{
    public PinnedMentorNode(string nodeTag) : base(nodeTag)
    {
    }

    public override ChosenNodeReason Reason => ChosenNodeReason.PinnedMentorNode;

    public override string ReasonForDecisionLog => $"Node '{NodeTag}' was selected because it's the pinned mentor node for this backup task";
}

public class NonExistingResponsibleNode : ResponsibleNodeForBackup
{
    public NonExistingResponsibleNode(string nodeTag) : base(nodeTag)
    {
    }

    public override ChosenNodeReason Reason => ChosenNodeReason.NonExistingResponsibleNode;

    public override string ReasonForDecisionLog => $"No responsible node for backup currently exists, setting it to '{NodeTag}'";
}

public class CurrentResponsibleNodeRemovedFromTopology : ResponsibleNodeForBackup
{
    private readonly string _currentResponsibleNode;

    public CurrentResponsibleNodeRemovedFromTopology(string nodeTag, string currentResponsibleNode) : base(nodeTag)
    {
        _currentResponsibleNode = currentResponsibleNode;
    }

    public override ChosenNodeReason Reason => ChosenNodeReason.CurrentResponsibleNodeRemovedFromTopology;

    public override string ReasonForDecisionLog => $"Node '{_currentResponsibleNode}' has been removed from topology. Node '{NodeTag}' will now be responsible for the backup task";
}

public class UnchangedResponsibleNode : ResponsibleNodeForBackup
{
    public UnchangedResponsibleNode(string nodeTag) : base(nodeTag)
    {
    }

    public override ChosenNodeReason Reason => ChosenNodeReason.UnchangedResponsibleNode;

    public override string ReasonForDecisionLog => null;
}

public class CurrentResponsibleNodeNotResponding : ResponsibleNodeForBackup
{
    private readonly string _currentResponsibleNode;
    private readonly TimeSpan _moveToNewResponsibleNodeGracePeriod;

    public CurrentResponsibleNodeNotResponding(string nodeTag, string currentResponsibleNode, TimeSpan moveToNewResponsibleNodeGracePeriod) : base(nodeTag)
    {
        _currentResponsibleNode = currentResponsibleNode;
        _moveToNewResponsibleNodeGracePeriod = moveToNewResponsibleNodeGracePeriod;
    }

    public override ChosenNodeReason Reason => ChosenNodeReason.CurrentResponsibleNodeNotResponding;

    public override string ReasonForDecisionLog => $"Node '{_currentResponsibleNode}' wasn't responding for {_moveToNewResponsibleNodeGracePeriod}. " +
                                                   $"Node '{NodeTag}' will now be responsible for the backup task";
}
