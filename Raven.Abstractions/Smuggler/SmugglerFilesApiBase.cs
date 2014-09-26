using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler.Data;

namespace Raven.Abstractions.Smuggler
{
    public class SmugglerFilesApiBase : ISmugglerApi<FilesConnectionStringOptions, SmugglerFilesOptions, FilesExportDataResult>
    {
        public SmugglerFilesOptions Options { get; private set; }

        public ISmugglerFilesOperations Operations { get; protected set; }

        protected SmugglerFilesApiBase(SmugglerFilesOptions options)
        {
            this.Options = options;
        }

        public virtual Task<FilesExportDataResult> ExportData(SmugglerExportOptions<FilesConnectionStringOptions> exportOptions)
        {
            throw new NotImplementedException();
        }

        public virtual Task ImportData(SmugglerImportOptions<FilesConnectionStringOptions> importOptions)
        {
            throw new NotImplementedException();
        }

        public virtual Task Between(SmugglerBetweenOptions<FilesConnectionStringOptions> betweenOptions)
        {
            throw new NotImplementedException();
        }
    }
}
