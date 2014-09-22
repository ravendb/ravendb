using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Smuggler;

namespace Raven.Smuggler
{
    public class SmugglerFilesApi : SmugglerFilesApiBase
    {
        public SmugglerFilesApi(SmugglerFilesOptions options = null) : base(options ?? new SmugglerFilesOptions())
        {
        }
    }
}
