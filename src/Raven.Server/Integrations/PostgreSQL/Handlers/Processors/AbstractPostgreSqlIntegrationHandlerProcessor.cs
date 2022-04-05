using JetBrains.Annotations;
using Raven.Client.Exceptions.Commercial;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Utils.Features;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Integrations.PostgreSQL.Handlers.Processors;

internal abstract class AbstractPostgreSqlIntegrationHandlerProcessor<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractPostgreSqlIntegrationHandlerProcessor([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    public static void AssertCanUsePostgreSqlIntegration(RequestHandler requestHandler)
    {
        if (requestHandler.ServerStore.LicenseManager.CanUsePowerBi(false, out _))
            return;

        if (requestHandler.ServerStore.LicenseManager.CanUsePostgreSqlIntegration(withNotification: true))
        {
            requestHandler.ServerStore.FeatureGuardian.Assert(Feature.PostgreSql, () => $"You have enabled the PostgreSQL integration via '{RavenConfiguration.GetKey(x => x.Integrations.PostgreSql.Enabled)}' configuration but " +
                                                                                        "this is an experimental feature and the server does not support experimental features. " +
                                                                                        $"Please enable experimental features by changing '{RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability)}' configuration value to '{nameof(FeaturesAvailability.Experimental)}'.");
        }

        throw new LicenseLimitException("You cannot use this feature because your license doesn't allow neither PostgreSQL integration feature nor Power BI");
    }
}
