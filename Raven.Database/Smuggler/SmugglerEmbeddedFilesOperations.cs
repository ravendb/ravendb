using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Smuggler;

namespace Raven.Database.Smuggler
{
    public class SmugglerEmbeddedFilesOperations : ISmugglerFilesOperations
    {
        public SmugglerFilesOptions Options { get; private set; }

        public SmugglerEmbeddedFilesOperations(SmugglerFilesOptions options)
        {
            Options = options;
        }
        
        public void Initialize(SmugglerFilesOptions options)
        {
            throw new NotImplementedException();
        }

        public void Configure(SmugglerFilesOptions options)
        {
            throw new NotImplementedException();
        }
    }
}
