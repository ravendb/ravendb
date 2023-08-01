using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class EditTimeSeriesConfigurationCommand : UpdateDatabaseCommand
    {
        public TimeSeriesConfiguration Configuration { get; private set; }

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
    }
}
