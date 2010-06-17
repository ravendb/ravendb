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
		public virtual Guid? Etag { get; set; }

#if !CLIENT
        public TransactionInformation TransactionInformation
        {
            get; set;
        }

    	public JObject Metadata
    	{
			get { return null; }
    	}

    	public void Execute(DocumentDatabase database)
    	{
    		database.Delete(Key, Etag, TransactionInformation);
    	}
#endif
    	public JObject ToJson()
    	{
    		return new JObject(
				new JProperty("Key", Key),
				new JProperty("Etag", new JValue(Etag != null ? (object)Etag.ToString() : null)),
				new JProperty("Method", Method)
				);
    	}
    }
}
