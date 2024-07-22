using System.Linq;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class EditTimeSeriesConfigurationCommand : UpdateDatabaseCommand
    {
        public TimeSeriesConfiguration Configuration { get; protected set; }

        public EditTimeSeriesConfigurationCommand()
        {
        }

        public EditTimeSeriesConfigurationCommand(TimeSeriesConfiguration configuration, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Configuration?.InitializeRollupAndRetention();

            record.TimeSeries = Configuration;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = Configuration.ToJson();
        }

        public override void AssertLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201, serverStore) == false)
                return;

            var licenseStatus = serverStore.Cluster.GetLicenseStatus(context);

            if (licenseStatus.HasTimeSeriesRollupsAndRetention)
                return;

            if (databaseRecord.TimeSeries == null)
                return;

            if (databaseRecord.TimeSeries.Collections.Count > 0 && databaseRecord.TimeSeries.Collections.All(x => x.Value.Disabled))
                return;

            throw new LicenseLimitException(LimitType.TimeSeriesRollupsAndRetention, "Your license doesn't support adding Time Series Rollups And Retention feature.");
        }
    }
}
