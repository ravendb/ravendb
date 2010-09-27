using System;

namespace Raven.Database
{
	/// <summary>
	/// The result of a PUT operation
	/// </summary>
    public class PutResult
    {
		/// <summary>
		/// Gets or sets the key.
		/// </summary>
		/// <value>The key.</value>
        public string Key { get; set; }
		/// <summary>
		/// Gets or sets the generated Etag for the PUT operation
		/// </summary>
		/// <value>The Etag.</value>
        public Guid ETag { get; set; }
    }
}