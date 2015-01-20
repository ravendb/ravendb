using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Counters
{

    public class CounterStorageDocument
    {
        /// <summary>
        /// The ID can be either the counters storage name ("CounterName") or the full document name ("Raven/Counters/CounterName").
        /// </summary>
        public string Id { get; set; }
        public Dictionary<string, string> Settings { get; set; }
        public Dictionary<string, string> SecuredSettings { get; set; } //preparation for air conditioner
        public bool Disabled { get; set; }

        public CounterStorageDocument()
        {
            Settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
			SecuredSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }
    }
}
