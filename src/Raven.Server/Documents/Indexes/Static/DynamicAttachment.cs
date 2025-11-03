using System;
using System.IO;
using System.Text;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Attachments;
using Raven.Server.Exceptions.Attachments;

namespace Raven.Server.Documents.Indexes.Static
{
    public sealed class DynamicAttachment : AbstractDynamicObject, IAttachmentObject
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

        public DateTime? RemoteAt
        {
            get
            {
                if (_attachment.RemoteParameters == null)
                {
                    return DateTime.MaxValue;
                }

                return _attachment.RemoteParameters.At;
            }
        }

        public RemoteAttachmentFlags RemoteFlags
        {
            get
            {
                if (_attachment.RemoteParameters == null)
                {
                    return RemoteAttachmentFlags.None;
                }

                return _attachment.RemoteParameters.Flags;
            }
        }

        public string RemoteIdentifier
        {
            get
            {
                if (_attachment.RemoteParameters == null)
                {
                    return DynamicNullObject.ExplicitNull;
                }

                return _attachment.RemoteParameters.Identifier;
            }
        }

        public string GetContentAsString()
        {
            return GetContentAsString(Encoding.UTF8);
        }

        public string GetContentAsString(Encoding encoding)
        {
            if (_attachment.RemoteParameters.IsRemoteStorageAttachment())
                ThrowRemoteAttachmentException(nameof(GetContentAsString));

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
            if (_attachment.RemoteParameters.IsRemoteStorageAttachment())
                ThrowRemoteAttachmentException(nameof(GetContentAsStream));

            _attachment.Stream.Position = 0;

            return _attachment.Stream;
        }

        protected override bool TryGetByName(string name, out object result)
        {
            result = DynamicNullObject.Null;
            return true;
        }

        private void ThrowRemoteAttachmentException(string methodName)
        {
            throw new RemoteAttachmentIndexingException(
                $"Attempted to {methodName} remote attachment '{_attachment.Name}' (storage id: {_attachment.StorageId});" +
                " remote attachments are no longer available locally.");
        }
    }
}
