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
			database.Put(Key, Etag, Document, Metadata,TransactionInformation);
		}
    }
}
