using Raven.Bundles.Attribution;

namespace Raven.Client.Attribution
{
    public static class AttributionClientExtensions
    {
        public static void AttributeTo(this IDocumentSession session, string userId)
        {
            session.Advanced.DatabaseCommands.OperationsHeaders[Constants.RavenAttributionUser] = userId;
        }
    }
}
