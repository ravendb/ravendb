//-----------------------------------------------------------------------
// <copyright file="PutCommandData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.SessionOperations.Commands
{
    /// <summary>
    /// A single batch operation for a document PUT
    /// </summary>
    public class PutCommandData : ICommandData
    {
        /// <summary>
        /// Key of a document.
        /// </summary>
        public virtual string Id { get; set; }

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
        public virtual long? Etag { get; set; }

        /// <summary>
        /// RavenJObject representing the document.
        /// </summary>
        public virtual BlittableJsonReaderObject Document { get; set; }

        /// <summary>
        /// Additional command data. For internal use only.
        /// </summary>
        public DynamicJsonValue AdditionalData { get; set; }

        /// <summary>
        /// Translates this instance to a Json object.
        /// </summary>
        /// <returns>RavenJObject representing the command.</returns>
        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                ["Key"] = Id,
                ["Method"] = Method,
                ["Document"] = Document,
                ["AdditionalData"] = AdditionalData,
            };
            if (Etag != null)
                json["Etag"] = Etag;
            return json;
        }
    }
}