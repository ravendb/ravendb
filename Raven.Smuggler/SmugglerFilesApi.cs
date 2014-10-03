using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Client.Document;
using Raven.Client.FileSystem;

namespace Raven.Smuggler
{
    public class SmugglerFilesApi : SmugglerFilesApiBase
    {
        private FilesStore store;
        private DocumentStore documentStore;

        public SmugglerFilesApi(SmugglerFilesOptions options = null) : base(options ?? new SmugglerFilesOptions())
        {
            Operations = new SmugglerRemoteFilesOperations(() => store, () => documentStore);
        }

        public override Task Between(SmugglerBetweenOptions<FilesConnectionStringOptions> betweenOptions)
        {
            return SmugglerFilesBetweenOperation.Between(betweenOptions, Options);
        }

        public override async Task<ExportFilesResult> ExportData(SmugglerExportOptions<FilesConnectionStringOptions> exportOptions)
        {
			using (store = CreateStores(exportOptions.From))
			{
				return await base.ExportData(exportOptions);
			}
        }

        public override async Task ImportData(SmugglerImportOptions<FilesConnectionStringOptions> importOptions)
        {
            using (store = CreateStores(importOptions.To))
            {
                await base.ImportData(importOptions);
            }
        }

        private FilesStore CreateStores(FilesConnectionStringOptions options)
        {
            throw new NotImplementedException();
        }
    }


}
