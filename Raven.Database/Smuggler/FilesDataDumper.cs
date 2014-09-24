using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Smuggler;

namespace Raven.Database.Smuggler
{
    public class FilesDataDumper : SmugglerFilesApiBase
    {
        public FilesDataDumper(ISmugglerFilesOperations operations, SmugglerFilesOptions options = null)
            : base(options ?? new SmugglerFilesOptions())
        {
            this.Operations = operations;
        }
    }
}
