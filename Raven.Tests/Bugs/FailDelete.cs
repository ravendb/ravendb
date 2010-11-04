using System;
using Newtonsoft.Json.Linq;

namespace Raven.Client.Tests.Bugs
{
    public class FailDelete : IDocumentDeleteListener
    {
        public void BeforeDelete(string key, object entityInstance, JObject metadata)
        {
            throw new NotImplementedException();
        }
    }
}