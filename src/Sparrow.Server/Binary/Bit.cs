using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Server.Binary
{
    public readonly struct Bit
    {
        public readonly byte Value;

        public bool IsSet => Value == 1;

        public Bit(byte value)
        {
            Value = (byte) (value & 1);
        }
    }
}
