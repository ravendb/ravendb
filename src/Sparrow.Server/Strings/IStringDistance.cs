using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Server.Strings
{
    public interface IStringDistance
    {
        float GetDistance(ReadOnlySpan<byte> target, ReadOnlySpan<byte> other);
    }
}
