using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Smuggler;

namespace Raven.Smuggler
{
    public class DatabaseSmugglerOperationDispatcher : SmugglerOperationDispatcher<SmugglerOptions>
    {
        private readonly SmugglerApi api;

        public DatabaseSmugglerOperationDispatcher(SmugglerApi api)
        {
            this.api = api;
        }


        protected override async Task PerformImportAsync(SmugglerOptions parameters)
        {
            await api.ImportData(new SmugglerImportOptions { FromFile = parameters.BackupPath, To = parameters.Source });
            if (parameters.WaitForIndexing)
                await api.WaitForIndexing();
        }

        protected override async Task PerformExportAsync(SmugglerOptions parameters)
        {
            await api.ExportData(new SmugglerExportOptions { From = parameters.Source, ToFile = parameters.BackupPath });
        }

        protected override async Task PerformBetweenAsync(SmugglerOptions parameters)
        {
            parameters.Destination.Url = parameters.BackupPath;
            await api.Between(new SmugglerBetweenOptions { From = parameters.Source, To = parameters.Destination });
        }
    }
}
