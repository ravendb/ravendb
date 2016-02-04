using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Data.RawData
{
    [Flags]
    public enum RawDataPageFlags : byte
    {
        None = 0,
        Header = 1,
        Small = 2,
        Large = 4,
    }
}
