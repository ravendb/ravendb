using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Configuration
{
    internal class ConfigurationHandlerProcessorForPostTimeSeriesConfiguration : AbstractConfigurationHandlerProcessorForPostTimeSeriesConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ConfigurationHandlerProcessorForPostTimeSeriesConfiguration([NotNull] DatabaseRequestHandler requestHandler)
            : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override string GetDatabaseName()
        {
            return RequestHandler.Database.Name;
        }
    }
}
