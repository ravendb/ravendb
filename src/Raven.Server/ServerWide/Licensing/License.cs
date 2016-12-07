using System;
using System.Collections.Generic;

namespace Raven.Server.ServerWide.Licensing
{
    public class License
    {
        public Guid Id { get; set; }

        public string Name { get; set; }

        public List<string> Keys { get; set; }
    }
}