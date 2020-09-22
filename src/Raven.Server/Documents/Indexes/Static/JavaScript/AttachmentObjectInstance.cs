using System;
using System.Collections.Generic;
using System.Text;
using Jint;
using Jint.Native;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Jint.Runtime.Interop;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Static.JavaScript
{
    public class AttachmentObjectInstance : ObjectInstanceBase
    {
        private const string GetContentAsStringMethodName = "getContentAsString";

        private readonly Dictionary<JsValue, PropertyDescriptor> _properties = new Dictionary<JsValue, PropertyDescriptor>();

        private readonly DynamicAttachment _attachment;

        public AttachmentObjectInstance(Engine engine, DynamicAttachment attachment) : base(engine)
        {
            _attachment = attachment ?? throw new ArgumentNullException(nameof(attachment));
        }

        private JsValue GetContentAsString(JsValue self, JsValue[] args)
        {
            var encoding = Encoding.UTF8;
            if (args.Length > 0)
            {
                if (args[0].IsString() == false)
                    throw new InvalidOperationException($"Encoding parameter must be of type string and convertible to one of the .NET supported encodings, but was '{args[0]}'.");

                var encodingAsString = args[0].AsString();
                if (string.Equals(encodingAsString, nameof(Encoding.UTF8), StringComparison.OrdinalIgnoreCase))
                    encoding = Encoding.UTF8;
                else if (string.Equals(encodingAsString, nameof(Encoding.Default), StringComparison.OrdinalIgnoreCase))
                    encoding = Encoding.Default;
                else if (string.Equals(encodingAsString, nameof(Encoding.ASCII), StringComparison.OrdinalIgnoreCase))
                    encoding = Encoding.ASCII;
                else if (string.Equals(encodingAsString, nameof(Encoding.BigEndianUnicode), StringComparison.OrdinalIgnoreCase))
                    encoding = Encoding.BigEndianUnicode;
                else if (string.Equals(encodingAsString, nameof(Encoding.Unicode), StringComparison.OrdinalIgnoreCase))
                    encoding = Encoding.Unicode;
                else if (string.Equals(encodingAsString, nameof(Encoding.UTF32), StringComparison.OrdinalIgnoreCase))
                    encoding = Encoding.UTF32;
#pragma warning disable SYSLIB0001 // Type or member is obsolete
                else if (string.Equals(encodingAsString, nameof(Encoding.UTF7), StringComparison.OrdinalIgnoreCase))
                    encoding = Encoding.UTF7;
#pragma warning restore SYSLIB0001 // Type or member is obsolete
                else
                    throw new InvalidOperationException($"Encoding parameter must be of type string and convertible to one of the .NET supported encodings, but was '{encodingAsString}'.");
            }

            return new JsString(_attachment.GetContentAsString(encoding));
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
                if (property == nameof(IAttachmentObject.Name))
                    value = new PropertyDescriptor(new JsString(_attachment.Name), writable: false, enumerable: false, configurable: false);
                else if (property == nameof(IAttachmentObject.ContentType))
                    value = new PropertyDescriptor(new JsString(_attachment.ContentType), writable: false, enumerable: false, configurable: false);
                else if (property == nameof(IAttachmentObject.Hash))
                    value = new PropertyDescriptor(new JsString(_attachment.Hash), writable: false, enumerable: false, configurable: false);
                else if (property == nameof(IAttachmentObject.Size))
                    value = new PropertyDescriptor(_attachment.Size, writable: false, enumerable: false, configurable: false);
                else if (property == GetContentAsStringMethodName)
                    value = new PropertyDescriptor(new ClrFunctionInstance(Engine, GetContentAsStringMethodName, GetContentAsString), writable: false, enumerable: false, configurable: false);

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

        public override bool HasOwnProperty(JsValue property)
        {
            throw new NotSupportedException();
        }

        public override bool HasProperty(JsValue property)
        {
            throw new NotSupportedException();
        }

        public override JsValue PreventExtensions()
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

        protected override void AddProperty(JsValue property, PropertyDescriptor descriptor)
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
