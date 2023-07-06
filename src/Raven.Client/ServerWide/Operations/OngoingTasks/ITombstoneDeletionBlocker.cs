namespace Raven.Client.ServerWide.Operations.OngoingTasks;

public interface ITombstoneDeletionBlocker
{
    string BlockingSourceName { get; }
    bool Disabled { get; set; }
}
