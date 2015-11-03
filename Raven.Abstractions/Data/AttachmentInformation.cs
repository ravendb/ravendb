//-----------------------------------------------------------------------
// <copyright file="AttachmentInformation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Data
{
    /// <summary>
    /// Describes an attachment, but without the actual attachment data
    /// </summary>
    [Obsolete("Use RavenFS instead.")]
    public class AttachmentInformation
    {
        /// <summary>
        /// Attachment size in bytes.
        /// <para>Remarks:</para>
        /// <para>- max size of an attachment can be 2GB</para>
        /// </summary>
        public int Size { get; set; }

        /// <summary>
        /// Key of an attachment.
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// RavenJObject representing attachment's metadata.
        /// </summary>
        public RavenJObject Metadata { get; set; }

        /// <summary>
        /// Current attachment etag.
        /// </summary>
        public Etag Etag { get; set; }
    }
}
