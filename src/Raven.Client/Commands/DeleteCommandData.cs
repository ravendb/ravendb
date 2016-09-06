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
        public virtual string Id { get; set; }

        /// <summary>
        /// Returns operation method. In this case DELETE.
        /// </summary>
        public string Method
        {
            get { return "DELETE"; }
        }

        /// <summary>
        /// Current document etag, used for concurrency checks (null to skip check)
        /// </summary>
        public virtual long? Etag { get; set; }

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
                        {"Key", Id},
                        {"Etag", new RavenJValue(Etag != null ? (object) Etag.ToString() : null)},
                        {"Method", Method},
                        {"AdditionalData", AdditionalData}
                    };
        }
    }
}
