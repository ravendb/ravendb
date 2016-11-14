using System;
using System.Collections.Generic;

namespace Raven.NewClient.Abstractions.Counters
{

    public class CounterStorageDocument
    {
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
