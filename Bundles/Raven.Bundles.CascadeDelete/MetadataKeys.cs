using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Bundles.CascadeDelete
{
    public static class MetadataKeys
    {
        public static readonly string DocumentsToCascadeDelete = "Cascade-Delete-Documents";
        public static readonly string AttachmentsToCascadeDelete = "Cascade-Delete-Attachments";
    }
}
