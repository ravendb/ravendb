using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Data
{
	/// <summary>
	/// A single batch operation for a document DELETE
	/// </summary>
    public class DeleteCommandData : ICommandData
    {
		/// <summary>
		/// Gets or sets the key.
		/// </summary>
		/// <value>The key.</value>
        public virtual string Key { get; set; }

		/// <summary>
		/// Gets the method.
		/// </summary>
		/// <value>The method.</value>
    	public string Method
    	{
			get { return "DELETE"; }
    	}
		/// <summary>
		/// Gets or sets the etag.
		/// </summary>
		/// <value>The etag.</value>
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
		/// <summary>
		/// Translate this instance to a Json object.
		/// </summary>
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
