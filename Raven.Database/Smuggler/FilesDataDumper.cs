using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Smuggler;
using Raven.Database.Server.RavenFS;

namespace Raven.Database.Smuggler
{
    public class FilesDataDumper : SmugglerFilesApiBase
    {
        public FilesDataDumper(RavenFileSystem filesystem, SmugglerFilesOptions options = null)
            : base(options ?? new SmugglerFilesOptions())
        {
            if (filesystem == null)
                throw new ArgumentNullException("filesystem");

            this.Operations = new SmugglerEmbeddedFilesOperations(filesystem);
        }
    }
}
