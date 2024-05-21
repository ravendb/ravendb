using System.Text;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client.Documents.Changes;
using Raven.Server.Documents.Queries;
using Raven.Server.Routing;
using static Raven.Server.RavenServer;

namespace Raven.Server.Web
{
    public abstract partial class RequestHandler
    {
        internal void TrafficWatchQuery(IndexQueryServerSide indexQuery)
        {
            TrafficWatchQuery(indexQuery, TrafficWatchChangeType.Queries);
        }

        internal void TrafficWatchStreamQuery(IndexQueryServerSide indexQuery)
        {
            TrafficWatchQuery(indexQuery, TrafficWatchChangeType.Streams);
        }

        private void TrafficWatchQuery(IndexQueryServerSide indexQuery, TrafficWatchChangeType type)
        {
            var sb = new StringBuilder();

            // append stringBuilder with the query
            sb.Append(indexQuery.Query);
            // if query got parameters append with parameters
            if (indexQuery.QueryParameters != null && indexQuery.QueryParameters.Count > 0)
                sb.AppendLine().Append(indexQuery.QueryParameters);

            AddStringToHttpContext(sb.ToString(), type);
        }

        public AuthorizationStatus GetAuthorizationStatusForSmuggler(string databaseName)
        {
            var authenticateConnection = HttpContext.Features.Get<IHttpAuthenticationFeature>() as AuthenticateConnection;
            return authenticateConnection?.GetAuthorizationStatus(databaseName) ?? AuthorizationStatus.DatabaseAdmin;
        }
    }
}
