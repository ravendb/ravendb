//-----------------------------------------------------------------------
// <copyright file="PatchCommandData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.NewClient.Client.Data;
using Sparrow.Json.Parsing;

namespace Raven.NewClient.Client.Document.Commands
{
    ///<summary>
    /// A single batch operation for a document EVAL (using a Javascript)
    ///</summary>
    public class PatchCommandData : ICommandData
    {
        /// <summary>
        /// ScriptedPatchRequest (using JavaScript) that is used to patch the document
        /// </summary>
        public PatchRequest Patch { get; set; }

        /// <summary>
        /// ScriptedPatchRequest (using JavaScript) that is used to patch a default document if the document is missing
        /// </summary>
        public PatchRequest PatchIfMissing { get; set; }

        /// <summary>
        /// Key of a document to patch.
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Returns operation method. In this case EVAL.
        /// </summary>
        public string Method
        {
            get { return "PATCH"; }
        }

        /// <summary>
        /// Current document etag, used for concurrency checks (null to skip check)
        /// </summary>
        public long? Etag { get; set; }

        /// <summary>
        /// Indicates in the operation should be run in debug mode. If set to true, then server will return additional information in response.
        /// </summary>
        public bool DebugMode { get; set; }

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
                ["Patch"] = new DynamicJsonValue
                {
                    ["Script"] = Patch.Script,
                    ["Values"] = Patch.Values,
                },
                ["DebugMode"] = DebugMode,
                ["AdditionalData"] = AdditionalData,
            };
            if (Etag != null)
                json["Etag"] = Etag;
            if (PatchIfMissing != null)
            {
                json["PatchIfMissing"] = new DynamicJsonValue
                {
                    ["Script"] = PatchIfMissing.Script,
                    ["Values"] = PatchIfMissing.Values,
                };
            }
            return json;
        }
    }
}
