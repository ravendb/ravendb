using System;
using System.IO;
using System.Text;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Static
{
    public class DynamicAttachment : AbstractDynamicObject, IAttachmentObject
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

        public string GetContentAsString()
        {
            return GetContentAsString(Encoding.UTF8);
        }

        public string GetContentAsString(Encoding encoding)
        {
            if (_contentAsString == null)
            {
                using (var sr = new StreamReader(_attachment.Stream, encoding))
                    _contentAsString = sr.ReadToEnd();
            }

            return _contentAsString;
        }

        protected override bool TryGetByName(string name, out object result)
        {
            result = DynamicNullObject.Null;
            return true;
        }
    }
}
