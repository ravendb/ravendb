using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Data.Fixed
{
    [Flags]
    public enum FixedSizeTreePageFlags : byte
    {
        None = 0,
        Branch = 1,
        Leaf = 2,
        Value = 4,
    }
}
