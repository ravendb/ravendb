namespace Raven.Server.Dashboard.Cluster
{
    public sealed class WidgetRequest
    {
        public string Command { get; set; }
        public int Id { get; set; }
        public ClusterDashboardNotificationType Type { get; set; }
        public object Config { get; set; }
    }
}