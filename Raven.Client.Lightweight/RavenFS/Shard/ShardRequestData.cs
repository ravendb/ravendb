using System.Collections.Generic;

namespace Raven.Client.RavenFS.Shard
{
	/// <summary>
	/// Information required to resolve the appropriate shard for an entity / entity and key
	/// </summary>
	public class ShardRequestData
	{
		/// <summary>
		/// Gets or sets the key.
		/// </summary>
		/// <value>The key.</value>
		public IList<string> Keys { get; set; }

        /// <summary>
        /// The query the user is using
        /// </summary>
	    public string Query { get; set; }

	    public ShardRequestData()
		{
			Keys = new List<string>();
		}
	}
}