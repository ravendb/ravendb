using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Data;

namespace Raven.Smuggler
{
    public class SmugglerFilesOperationDispatcher : SmugglerOperationDispatcher<SmugglerFilesOptions>
    {
        private readonly SmugglerFilesApiBase api;

        public SmugglerFilesOperationDispatcher(SmugglerFilesApiBase api)
            : base(api.Options)
        {
            this.api = api;
        }


        protected override async Task PerformImportAsync(SmugglerFilesOptions parameters)
        {
            await api.ImportData(new SmugglerImportOptions<FilesConnectionStringOptions> { FromFile = parameters.BackupPath, To = parameters.Source }).ConfigureAwait(false);            
        }

        protected override async Task PerformExportAsync(SmugglerFilesOptions parameters)
        {
            await api.ExportData(new SmugglerExportOptions<FilesConnectionStringOptions> { From = parameters.Source, ToFile = parameters.BackupPath }).ConfigureAwait(false);
        }

        protected override async Task PerformBetweenAsync(SmugglerFilesOptions parameters)
        {
            parameters.Destination.Url = parameters.BackupPath;
            await api.Between(new SmugglerBetweenOptions<FilesConnectionStringOptions> { From = parameters.Source, To = parameters.Destination }).ConfigureAwait(false);
        }

        protected override string FileExtension => "ravenfsdump";
    }
}
