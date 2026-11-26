namespace Raven.Server.Routing
{
    /// <summary>
    /// The routes of the server, generated at compile time by Raven.Server.SourceGenerators from the [RavenAction] and [RavenShardedAction] methods.
    /// </summary>
    internal static partial class RouteTable
    {
    }

    internal readonly struct GeneratedRoute(string name, bool isDatabaseHandler, HandleRequest handler, RavenActionAttribute attribute)
    {
        /// <summary>
        /// The handler type and method, for error messages.
        /// </summary>
        public readonly string Name = name;

        public readonly bool IsDatabaseHandler = isDatabaseHandler;

        public readonly HandleRequest Handler = handler;

        public readonly RavenActionAttribute Attribute = attribute;
    }

    internal readonly struct GeneratedShardedRoute(string name, HandleRequest handler, RavenShardedActionAttribute attribute)
    {
        public readonly string Name = name;

        public readonly HandleRequest Handler = handler;

        public readonly RavenShardedActionAttribute Attribute = attribute;
    }
}
