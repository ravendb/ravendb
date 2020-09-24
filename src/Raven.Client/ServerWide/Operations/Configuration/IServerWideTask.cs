namespace Raven.Client.ServerWide.Operations.Configuration
{
    public interface IServerWideTask
    {
        string[] ExcludedDatabases { get; set; }
    }
}
