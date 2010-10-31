using System;
using Newtonsoft.Json.Linq;
using Raven.Http;

namespace Raven.Database.Data
{
	/// <summary>
	/// A single batch operation for a document PUT
	/// </summary>
    public class PutCommandData : ICommandData
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
			get { return "PUT"; }
    	}

    	public TransactionInformation TransactionInformation
        {
            get;
            set;
        }

		/// <summary>
		/// Gets or sets the etag.
		/// </summary>
		/// <value>The etag.</value>
        public virtual Guid? Etag { get; set; }
		/// <summary>
		/// Gets or sets the document.
		/// </summary>
		/// <value>The document.</value>
        public virtual JObject Document { get; set; }
		/// <summary>
		/// Gets or sets the metadata.
		/// </summary>
		/// <value>The metadata.</value>
        public virtual JObject Metadata { get; set; }


    	/// <summary>
		/// Translate this instance to a Json object.
		/// </summary>
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
