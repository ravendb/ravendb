using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Smuggler;

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


        protected override Task PerformImportAsync(SmugglerFilesOptions parameters)
        {
            throw new NotImplementedException();
        }

        protected override Task PerformExportAsync(SmugglerFilesOptions parameters)
        {
            throw new NotImplementedException();
        }

        protected override Task PerformBetweenAsync(SmugglerFilesOptions parameters)
        {
            throw new NotImplementedException();
        }
    }
}
