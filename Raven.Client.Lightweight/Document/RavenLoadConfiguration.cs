using System.Collections.Generic;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
    public class RavenLoadConfiguration : ILoadConfiguration
    {
        public Dictionary<string, RavenJToken> QueryInputs { get; set; }

        public RavenLoadConfiguration()
        {
            QueryInputs = new Dictionary<string, RavenJToken>();
        }

        public void AddQueryParam(string name, RavenJToken value)
        {
            QueryInputs[name] = value;
        }
    }
}