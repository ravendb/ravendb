using System;
using Newtonsoft.Json.Linq;
using Raven.Client;

namespace Raven.Tests.Bugs
{
    public class FailDelete : IDocumentDeleteListener
    {
        public void BeforeDelete(string key, object entityInstance, JObject metadata)
        {
            throw new NotImplementedException();
        }
    }
}