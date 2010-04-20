using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Data
{
    public interface ICommandData
    {
		string Key { get; }
		string Method { get; }
		Guid? Etag { get; }
		TransactionInformation TransactionInformation { get; set; }
    	void Execute(DocumentDatabase database);
    	JObject ToJson();
    }
}
