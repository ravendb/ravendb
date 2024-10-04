using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Raven.Client.Documents.Operations.Attachments;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static.JavaScript
{
    public sealed class AttachmentNameObjectInstance : ObjectInstanceBase
    {
        private readonly Dictionary<JsValue, PropertyDescriptor> _properties = new Dictionary<JsValue, PropertyDescriptor>();

        private readonly BlittableJsonReaderObject _attachmentName;

        public AttachmentNameObjectInstance(Engine engine, BlittableJsonReaderObject attachmentName) : base(engine)
        {
            _attachmentName = attachmentName ?? throw new ArgumentNullException(nameof(attachmentName));
        }

        public override bool DefineOwnProperty(JsValue property, PropertyDescriptor desc)
        {
            throw new NotSupportedException();
        }

        public override bool Delete(JsValue property)
        {
            throw new NotSupportedException();
        }

        public override IEnumerable<KeyValuePair<JsValue, PropertyDescriptor>> GetOwnProperties()
        {
            throw new NotSupportedException();
        }

        public override PropertyDescriptor GetOwnProperty(JsValue property)
        {
            if (_properties.TryGetValue(property, out var value) == false)
            {
                if (property == nameof(AttachmentName.Name) && _attachmentName.TryGet(nameof(AttachmentName.Name), out string name))
                    value = new PropertyDescriptor(new JsString(name), writable: false, enumerable: false, configurable: false);
                else if (property == nameof(AttachmentName.ContentType) && _attachmentName.TryGet(nameof(AttachmentName.ContentType), out string contentType))
                    value = new PropertyDescriptor(new JsString(contentType), writable: false, enumerable: false, configurable: false);
                else if (property == nameof(AttachmentName.Hash) && _attachmentName.TryGet(nameof(AttachmentName.Hash), out string hash))
                    value = new PropertyDescriptor(new JsString(hash), writable: false, enumerable: false, configurable: false);
                else if (property == nameof(AttachmentName.Size) && _attachmentName.TryGet(nameof(AttachmentName.Size), out long size))
                    value = new PropertyDescriptor(size, writable: false, enumerable: false, configurable: false);

                if (value != null)
                    _properties[property] = value;
            }

            if (value == null)
                value = ImplicitNull;

            return value;
        }

        public override List<JsValue> GetOwnPropertyKeys(Types types = Types.String | Types.Symbol)
        {
            throw new NotSupportedException();
        }

        public override bool HasProperty(JsValue property)
        {
            throw new NotSupportedException();
        }

        public override bool PreventExtensions()
        {
            throw new NotSupportedException();
        }

        public override void RemoveOwnProperty(JsValue property)
        {
            throw new NotSupportedException();
        }

        public override bool Set(JsValue property, JsValue value, JsValue receiver)
        {
            throw new NotSupportedException();
        }

        protected override void SetOwnProperty(JsValue property, PropertyDescriptor desc)
        {
            throw new NotSupportedException();
        }

        protected override bool TryGetProperty(JsValue property, out PropertyDescriptor descriptor)
        {
            throw new NotSupportedException();
        }
    }
}
