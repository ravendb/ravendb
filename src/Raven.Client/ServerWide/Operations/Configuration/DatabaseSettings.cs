using System.Collections.Generic;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public sealed class DatabaseSettings
    {
        public Dictionary<string, string> Settings { get; set; }
    }
}
