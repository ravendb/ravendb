//-----------------------------------------------------------------------
// <copyright file="DeleteCommandData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Abstractions.Commands
{
    /// <summary>
    /// A single batch operation for a document DELETE
    /// </summary>
    public class DeleteCommandData : ICommandData
    {
        /// <summary>
        /// Key of a document to delete.
        /// </summary>
        public virtual string Key { get; set; }

        /// <summary>
        /// Returns operation method. In this case DELETE.
        /// </summary>
        public string Method => "DELETE";

        /// <summary>
        /// Current document etag, used for concurrency checks (null to skip check)
        /// </summary>
        public virtual Etag Etag { get; set; }

        /// <summary>
        /// Information used to identify a transaction. Contains transaction Id and timeout.
        /// </summary>
        public TransactionInformation TransactionInformation { get; set; }

        /// <summary>
        /// RavenJObject representing document's metadata. In this case null.
        /// </summary>
        public RavenJObject Metadata
        {
            get { return null; }
        }

        /// <summary>
        /// Additional command data. For internal use only.
        /// </summary>
        public RavenJObject AdditionalData { get; set; }

        /// <summary>
        /// Translates this instance to a Json object.
        /// </summary>
        /// <returns>RavenJObject representing the command.</returns>
        public RavenJObject ToJson()
        {
            return new RavenJObject
                    {
                        {"Key", Key},
                        {"Etag", new RavenJValue(Etag != null ? (object) Etag.ToString() : null)},
                        {"Method", Method},
                        {"AdditionalData", AdditionalData}
                    };
        }
    }
}
