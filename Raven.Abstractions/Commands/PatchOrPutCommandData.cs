using System.Linq;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Abstractions.Commands
{
	/// <summary>
	/// A single batch operation to PATCH a document if it exists or PUT a document if it does not
	/// </summary>
	public class PatchOrPutCommandData : ICommandData
	{
		/// <summary>
		/// Gets or sets the patches applied to this document
		/// </summary>
		/// <value>The patches.</value>
		public PatchRequest[] Patches{ get; set;}

		/// <summary>
		/// Gets or sets the document.
		/// </summary>
		/// <value>The document.</value>
		public virtual RavenJObject Document { get; set; }

		/// <summary>
		/// Gets the key.
		/// </summary>
		/// <value>The key.</value>
		public string Key
		{
			get;
			set;
		}

		/// <summary>
		/// Gets the method.
		/// </summary>
		/// <value>The method.</value>
		public string Method
		{
			get { return "PATCHPUT"; }
		}

		/// <summary>
		/// Gets or sets the etag.
		/// </summary>
		/// <value>The etag.</value>
		public Etag Etag
		{
			get;
			set;
		}

		/// <summary>
		/// Gets the transaction information.
		/// </summary>
		/// <value>The transaction information.</value>
		public TransactionInformation TransactionInformation
		{
			get;
			set;
		}

		/// <summary>
		/// Gets or sets the metadata.
		/// </summary>
		/// <value>The metadata.</value>
		public RavenJObject Metadata
		{
			get;
			set;
		}

		public RavenJObject AdditionalData { get; set; }

		public RavenJObject ToJson()
		{
			var ret = new RavenJObject
					{
						{"Key", Key},
						{"Method", Method},
						{"Patches", new RavenJArray(Patches.Select(x => x.ToJson()))},
						{"Document", Document},
						{"Metadata", Metadata},
						{"AdditionalData", AdditionalData}
					};
			if (Etag != null)
				ret.Add("Etag", Etag.ToString());
			return ret;
		}
	}
}