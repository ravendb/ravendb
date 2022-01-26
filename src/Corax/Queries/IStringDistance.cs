using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Voron;

namespace Corax.Queries
{
    public interface IStringDistance
    {
        float GetDistance(Slice target, Slice other);
    }
}
