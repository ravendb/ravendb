using System.Collections.Generic;


namespace Raven.Client.Documents
{
    public class RavenLoadConfiguration : ILoadConfiguration
    {
        public Dictionary<string, object> TransformerParameters { get; set; }

        public RavenLoadConfiguration()
        {
            TransformerParameters = new Dictionary<string, object>();
        }

        public void AddQueryParam(string name, object value)
        {
            AddTransformerParameter(name, value);
        }

        public void AddTransformerParameter(string name, object value)
        {
            TransformerParameters[name] = value;
        }
    }
}
