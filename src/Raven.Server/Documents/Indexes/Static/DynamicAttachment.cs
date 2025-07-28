using System;
using System.IO;
using System.Text;
using Raven.Client.Documents.Attachments;
using Raven.Server.Documents.Indexes.Static.Attachments;
using Raven.Server.Exceptions;

namespace Raven.Server.Documents.Indexes.Static
{
    public sealed class DynamicAttachment : AbstractDynamicObject, IAttachmentIndexObject
    {
        private readonly Attachment _attachment;

        private string _hash;

        private string _contentAsString;

        public DynamicAttachment(Attachment attachment)
        {
            _attachment = attachment ?? throw new ArgumentNullException(nameof(attachment));
        }

        public override dynamic GetId()
        {
            throw new NotSupportedException();
        }

        public override bool Set(object item)
        {
            throw new NotSupportedException();
        }

        public string Name
        {
            get
            {
                return _attachment.Name;
            }
        }

        public string ContentType
        {
            get
            {
                return _attachment.ContentType;
            }
        }

        public string Hash
        {
            get
            {
                if (_hash == null)
                    _hash = _attachment.Base64Hash.ToString();

                return _hash;
            }
        }

        public long Size
        {
            get
            {
                return _attachment.Size;
            }
        }

        public dynamic RetireAt
        {
            get
            {
                if (_attachment.IsRetired() == false)
                {
                    return DynamicNullObject.ExplicitNull;
                }

                return _attachment.RetireParameters.At;
            }
        }

        public RetiredAttachmentFlags Flags
        {
            get
            {
                if (_attachment.IsRetired() == false)
                {
                    return RetiredAttachmentFlags.None;
                }

                return _attachment.RetireParameters.Flags;
            }
        }

        public string GetContentAsString()
        {
            return GetContentAsString(Encoding.UTF8);
        }

        public string GetContentAsString(Encoding encoding)
        {
            if (_attachment.IsRetired())
                ThrowRetiredAttachmentException(nameof(GetContentAsString));

            if (_contentAsString == null)
            {
                _attachment.Stream.Position = 0;

                using (var sr = new StreamReader(_attachment.Stream, encoding))
                    _contentAsString = sr.ReadToEnd();
            }

            return _contentAsString;
        }

        public Stream GetContentAsStream()
        {
            if (_attachment.IsRetired())
                ThrowRetiredAttachmentException(nameof(GetContentAsStream));

            _attachment.Stream.Position = 0;

            return _attachment.Stream;
        }

        protected override bool TryGetByName(string name, out object result)
        {
            result = DynamicNullObject.Null;
            return true;
        }

        private void ThrowRetiredAttachmentException(string methodName)
        {
            throw new RetiredAttachmentIndexingException(
                $"Attempted to {methodName} retired attachment '{_attachment.Name}' (storage id: {_attachment.StorageId});" +
                " retired attachments are no longer available locally.");
        }
    }
}
