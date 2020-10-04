namespace Raven.Client.ServerWide.Operations.OngoingTasks
{
    public interface IServerWideTask
    {
        string[] ExcludedDatabases { get; set; }
    }
}
