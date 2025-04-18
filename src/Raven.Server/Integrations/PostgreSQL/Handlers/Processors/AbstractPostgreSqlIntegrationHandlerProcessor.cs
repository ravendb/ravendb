using JetBrains.Annotations;
using Raven.Client.Exceptions.Commercial;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Utils.Features;
using Sparrow.Json;

namespace Raven.Server.Integrations.PostgreSQL.Handlers.Processors;

internal abstract class AbstractPostgreSqlIntegrationHandlerProcessor<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractPostgreSqlIntegrationHandlerProcessor([NotNull] TRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    public static void AssertCanUsePostgreSqlIntegration(TRequestHandler requestHandler)
    {
        if (requestHandler.ServerStore.LicenseManager.CanUsePowerBi(false, out _))
            return;

        if (requestHandler.ServerStore.LicenseManager.CanUsePostgreSqlIntegration(withNotification: true))
        {
            // if Postgres is enabled in config, we need to alert it's an experimental feature
            if (requestHandler.ServerStore.Configuration.Integrations.PostgreSql.Enabled)
            {
                requestHandler.ServerStore.FeatureGuardian.Assert(Feature.PostgreSql, () =>
                    $"You have enabled the PostgreSQL integration via '{RavenConfiguration.GetKey(x => x.Integrations.PostgreSql.Enabled)}' configuration but " +
                    "this is an experimental feature and the current server configuration does not allow to use experimental features. " +
                    $"Please enable experimental features by changing '{RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability)}' configuration value to '{nameof(FeaturesAvailability.Experimental)}'.");
            }
            // if license check for Postgres passed, but it's still disabled in the configuration, we don't need to alert about anything yet
            return;
        }

        throw new LicenseLimitException("You cannot use this feature because your license doesn't allow neither PostgreSQL integration feature nor Power BI");
    }
}
