using System;
using System.Globalization;
using System.IO;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents
{
    public sealed class Attachment
    {
        public long StorageId;
        public LazyStringValue Key;
        public long Etag;
        public string ChangeVector;
        public LazyStringValue Name;
        public LazyStringValue ContentType;
        public Slice Base64Hash;
        public Stream Stream;
        public short TransactionMarker;
        public long Size;
        public RetireAttachmentParameters RetireParameters;

        public static RetireAttachmentParameters GetRetireAttachmentParameters(string identifier, DateTime? retireAt, AttachmentFlags flags)
        {
            RetireAttachmentParameters retireParameters = null;
            if (retireAt.HasValue)
            {
                retireParameters = new RetireAttachmentParameters(identifier, retireAt.Value) { Flags = flags };
            }

            return retireParameters;
        }

        public static RetireAttachmentParameters GetRetireAttachmentParameters(LazyStringValue identifier, DateTime? retireAt, AttachmentFlags flags)
        {
            return GetRetireAttachmentParameters(identifier.ToString(CultureInfo.CurrentCulture), retireAt, flags);
        }

        public bool IsRetired()
        {
            if (RetireParameters == null)
            {
                return false;
            }

            return RetireParameters.Flags != AttachmentFlags.None;
        }
    }
}
