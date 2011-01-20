namespace Raven.Management.Client.Silverlight.Document
{
    using System;
    using Newtonsoft.Json.Linq;

    public class DocumentMetadata
    {
        /// <summary>
        /// Gets or sets the original value.
        /// </summary>
        /// <value>The original value.</value>
        public JObject OriginalValue { get; set; }

        /// <summary>
        /// Gets or sets the metadata.
        /// </summary>
        /// <value>The metadata.</value>
        public JObject Metadata { get; set; }

        /// <summary>
        /// Gets or sets the ETag.
        /// </summary>
        /// <value>The ETag.</value>
        public Guid? ETag { get; set; }

        /// <summary>
        /// Gets or sets the key.
        /// </summary>
        /// <value>The key.</value>
        public string Key { get; set; }

        /// <summary>
        /// Gets or sets the original metadata.
        /// </summary>
        /// <value>The original metadata.</value>
        public JObject OriginalMetadata { get; set; }
    }
}