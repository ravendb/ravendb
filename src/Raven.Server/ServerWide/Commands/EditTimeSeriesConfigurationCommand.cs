using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.ServerWide;
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

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (Configuration?.Collections != null)
            {
                foreach (var config in Configuration.Collections.Values)
                {
                    config.Validate();
                }
            }

            record.TimeSeries = Configuration;
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = Configuration.ToJson();
        }
    }
}
