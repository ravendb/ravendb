using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Data
{
    public interface ICommandData
    {
		string Key { get; }
		string Method { get; }
		Guid? Etag { get; }
#if !CLIENT
		TransactionInformation TransactionInformation { get; set; }
    	void Execute(DocumentDatabase database);
#endif
    	JObject ToJson();
    }
}
