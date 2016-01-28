// -----------------------------------------------------------------------
//  <copyright file="SmugglerExportType.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel;

namespace Raven.Abstractions.Smuggler
{
    public enum SmugglerExportType
    {
        [Description("Docs")]
        Document,

        [Description("Attachments")]
        Attachment,

        [Description("Identities")]
        Identity,

        [Description("DocsDeletions")]
        DocumentDeletion,

        [Description("AttachmentsDeletions")]
        AttachmentDeletion,

        [Description("Summary")]
        Summary
    }
}