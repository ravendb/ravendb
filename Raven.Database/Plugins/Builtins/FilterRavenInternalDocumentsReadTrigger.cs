using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Plugins.Builtins
{
    public class FilterRavenInternalDocumentsReadTrigger : AbstractReadTrigger
    {
        public override ReadVetoResult AllowRead(string key, JObject document, JObject metadata, ReadOperation operation, TransactionInformation transactionInformation)
        {
            if(key == null)
                return ReadVetoResult.Allowed;
            if (key.StartsWith("Raven/"))
            {
            	switch (operation)
            	{
            		case ReadOperation.Load:
            			return ReadVetoResult.Allowed;
            		case ReadOperation.Query:
            		case ReadOperation.Index:
            			return ReadVetoResult.Ignore;
            		default:
            			throw new ArgumentOutOfRangeException("operation");
            	}
            }
            return ReadVetoResult.Allowed;
        }
    }
}
