using System.Collections.Generic;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Document
{
    public class RavenLoadConfiguration : ILoadConfiguration
    {
        public Dictionary<string, RavenJToken> TransformerParameters { get; set; }

        public RavenLoadConfiguration()
        {
            TransformerParameters = new Dictionary<string, RavenJToken>();
        }

        public void AddQueryParam(string name, RavenJToken value)
        {
            AddTransformerParameter(name, value);
        }

        public void AddTransformerParameter(string name, RavenJToken value)
        {
            TransformerParameters[name] = value;
        }
    }
}
