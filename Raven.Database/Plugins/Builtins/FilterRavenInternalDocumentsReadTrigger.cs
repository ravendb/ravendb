using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Plugins.Builtins
{
    public class FilterRavenInternalDocumentsReadTrigger : IReadTrigger
    {
        public ReadVetoResult AllowRead(string key, JObject document, JObject metadata, ReadOperation operation, TransactionInformation transactionInformation)
        {
            if(key == null)
                return ReadVetoResult.Allowed;
            if (key.StartsWith("Raven/") && operation == ReadOperation.Query)
                return ReadVetoResult.Ignore; // hide Raven's documentation from queries
            return ReadVetoResult.Allowed;
        }

        public void OnRead(string key, JObject document, JObject metadata, ReadOperation operation, TransactionInformation transactionInformation)
        {
        }
    }
}