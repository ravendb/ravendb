using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Counters
{
    public class CounterTotal
    {
        public bool IsExists { get; set; }
        public long Total { get; set; }
    }
}
