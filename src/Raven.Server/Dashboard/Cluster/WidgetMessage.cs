using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster
{
    public sealed class WidgetMessage
    {
        public int Id { get; set; }
        public DynamicJsonValue Data { get; set; }
    }
}