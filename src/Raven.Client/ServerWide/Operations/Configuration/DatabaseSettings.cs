using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class DatabaseSettings
    {
        public Dictionary<string, string> Settings { get; set; }
    }
}
