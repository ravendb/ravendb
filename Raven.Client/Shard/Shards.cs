using System.Collections.Generic;
using Raven.Client.Document;

namespace Raven.Client.Shard
{
	/// <summary>
	/// Referest a list of shards
	/// </summary>
    public class Shards : List<DocumentStore>
    {
		/// <summary>
		/// Initializes a new instance of the <see cref="Shards"/> class.
		/// </summary>
        public Shards()
        {

        }

		/// <summary>
		/// Initializes a new instance of the <see cref="Shards"/> class.
		/// </summary>
		/// <param name="shards">The shards.</param>
        public Shards(IEnumerable<DocumentStore> shards) : base(shards)
        {

        }
    }
}
