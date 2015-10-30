//-----------------------------------------------------------------------
// <copyright file="PutCommandData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Abstractions.Commands
{
    /// <summary>
    /// A single batch operation for a document PUT
    /// </summary>
    public class PutCommandData : ICommandData
    {
        /// <summary>
        /// Key of a document.
        /// </summary>
        public virtual string Key { get; set; }

        /// <summary>
        /// Returns operation method. In this case PUT.
        /// </summary>
        public string Method
        {
            get { return "PUT"; }
        }

        /// <summary>
        /// Current document etag, used for concurrency checks (null to skip check)
        /// </summary>
        public virtual Etag Etag { get; set; }

        /// <summary>
        /// RavenJObject representing the document.
        /// </summary>
        public virtual RavenJObject Document { get; set; }

        /// <summary>
        /// Information used to identify a transaction. Contains transaction Id and timeout.
        /// </summary>
        public TransactionInformation TransactionInformation { get; set; }

        /// <summary>
        /// RavenJObject representing document's metadata.
        /// </summary>
        public virtual RavenJObject Metadata { get; set; }

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
            var ret = new RavenJObject
            {
                {"Key", Key},
                {"Method", Method},
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
