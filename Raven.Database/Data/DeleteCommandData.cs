using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Data
{
    public class DeleteCommandData : ICommandData
    {
        public virtual string Key { get; set; }

    	public string Method
    	{
			get { return "DELETE"; }
    	}

        public TransactionInformation TransactionInformation
        {
            get; set;
        }

        public virtual Guid? Etag { get; set; }

    	public void Execute(DocumentDatabase database)
    	{
    		database.Delete(Key, Etag, TransactionInformation);
    	}

    	public JObject ToJson()
    	{
    		return new JObject(
				new JProperty("key", Key),
				new JProperty("etag", new JValue(Etag != null ? (object)Etag.ToString() : null)),
				new JProperty("method", Method)
				);
    	}
    }
}
