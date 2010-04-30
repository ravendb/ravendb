using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Data
{
    public class PutCommandData : ICommandData
    {
        public virtual string Key { get; set; }

    	public string Method
    	{
			get { return "PUT"; }
    	}

    	public TransactionInformation TransactionInformation
        {
            get;
            set;
        }

        public virtual Guid? Etag { get; set; }
        public virtual JObject Document { get; set; }
        public virtual JObject Metadata { get; set; }

		public void Execute(DocumentDatabase database)
		{
			var putResult = database.Put(Key, Etag, Document, Metadata, TransactionInformation);
			Etag = putResult.ETag;
			Key = putResult.Key;
		}

    	public JObject ToJson()
    	{
    		return new JObject(
				new JProperty("Key", new JValue((object)Key)),
    			new JProperty("Etag", new JValue(Etag != null ? (object)Etag.ToString() : null)),
    			new JProperty("Method", new JValue(Method)),
				new JProperty("Document", Document),
				new JProperty("Metadata", Metadata)
    			);
    	}
    }
}
