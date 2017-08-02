using System;
using System.Collections.Generic;
using Sparrow.Extensions;

namespace Raven.Client.Documents.Session
{
    public class LoadConfiguration : ILoadConfiguration
    {
        public Dictionary<string, object> TransformerParameters { get; set; }

        public LoadConfiguration()
        {
            TransformerParameters = new Dictionary<string, object>();
        }

        public void AddTransformerParameter(string name, object value)
        {
            TransformerParameters[name] = value;
        }

        public void AddTransformerParameter(string name, DateTime value)
        {
            TransformerParameters[name] = value.GetDefaultRavenFormat(isUtc: value.Kind == DateTimeKind.Utc);
        }
    }
}
