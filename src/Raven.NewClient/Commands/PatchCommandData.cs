//-----------------------------------------------------------------------
// <copyright file="PatchCommandData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Abstractions.Commands
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
        public RavenJObject AdditionalData { get; set; }

        /// <summary>
        /// Translates this instance to a Json object.
        /// </summary>
        /// <returns>RavenJObject representing the command.</returns>
        public RavenJObject ToJson()
        {
            var ret = new RavenJObject
                    {
                        {"Key", Id},
                        {"Method", Method},
                        {"Patch", new RavenJObject
                        {
                            { "Script", Patch.Script },
                            { "Values", RavenJObject.FromObject(Patch.Values)}
                        }},
                        {"DebugMode", DebugMode},
                        {"AdditionalData", AdditionalData},
                    };
            if (Etag != null)
                ret.Add("Etag", Etag.ToString());
            if (PatchIfMissing != null)
            {
                ret.Add("PatchIfMissing", new RavenJObject
                        {
                            { "Script", PatchIfMissing.Script },
                            { "Values", RavenJObject.FromObject(PatchIfMissing.Values)}
                        });
            }
            return ret;
        }
    }
}
