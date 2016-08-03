using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;

namespace Raven.Smuggler
{
    public class SmugglerDatabaseOperationDispatcher : SmugglerOperationDispatcher<SmugglerDatabaseOptions>
    {
        private readonly SmugglerDatabaseApi api;

        public SmugglerDatabaseOperationDispatcher(SmugglerDatabaseApi api)
            : base(api.Options)
        {
            this.api = api;
        }


        protected override async Task PerformImportAsync(SmugglerDatabaseOptions parameters)
        {
            await api.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = parameters.BackupPath, To = parameters.Source }).ConfigureAwait(false);
            if (parameters.WaitForIndexing)
                await api.WaitForIndexing().ConfigureAwait(false);
        }

        protected override async Task PerformExportAsync(SmugglerDatabaseOptions parameters)
        {
            await api.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { From = parameters.Source, ToFile = parameters.BackupPath }).ConfigureAwait(false);
        }

        protected override async Task PerformBetweenAsync(SmugglerDatabaseOptions parameters)
        {
            parameters.Destination.Url = parameters.BackupPath;
            await api.Between(new SmugglerBetweenOptions<RavenConnectionStringOptions> { From = parameters.Source, To = parameters.Destination }).ConfigureAwait(false);
        }

        protected override string FileExtension => "ravendbdump";
    }
}
