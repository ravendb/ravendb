using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Smuggler
{
    public interface ISmugglerFilesOperations
    {
        SmugglerFilesOptions Options { get; }

        void Initialize(SmugglerFilesOptions options);

        void Configure(SmugglerFilesOptions options);
    }
}
