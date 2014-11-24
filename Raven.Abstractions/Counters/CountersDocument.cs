using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Counters
{

    public class CountersDocument
    {
        /// <summary>
        /// The ID can be either the counters storage name ("CounterName") or the full document name ("Raven/Counters/CounterName").
        /// </summary>
        public string Id { get; set; }
        public Dictionary<string, string> Settings { get; set; }
        public Dictionary<string, string> SecuredSettings { get; set; }
        public bool Disabled { get; set; }

        public CountersDocument()
        {
            Settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }
    }
}
