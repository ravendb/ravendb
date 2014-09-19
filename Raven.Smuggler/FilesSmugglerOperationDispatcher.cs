using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Smuggler;

namespace Raven.Smuggler
{
    public class FilesSmugglerOperationDispatcher: SmugglerOperationDispatcher<SmugglerOptions>
    {
        protected override Task PerformImportAsync(SmugglerOptions parameters)
        {
            throw new NotImplementedException();
        }

        protected override Task PerformExportAsync(SmugglerOptions parameters)
        {
            throw new NotImplementedException();
        }

        protected override Task PerformBetweenAsync(SmugglerOptions parameters)
        {
            throw new NotImplementedException();
        }
    }
}
