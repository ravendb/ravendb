using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Indexes;

using V8.Net;

using Raven.Server.Documents.Patch.V8;

namespace Raven.Server.Documents.Indexes.Static.JavaScript.V8
{
    //[ScriptObject("AttachmentObjectInstance", ScriptMemberSecurity.NoAcccess)]
    public class AttachmentObjectInstance : PropertiesObjectInstance
    {
        private const string GetContentAsStringMethodName = "getContentAsString";

        private readonly DynamicAttachment _attachment;

        public AttachmentObjectInstance() : base()
        {
            assert(false);
        }
        
        public AttachmentObjectInstance(DynamicAttachment attachment) : base()
        {
            _attachment = attachment ?? throw new ArgumentNullException(nameof(attachment));
        }

        private InternalHandle GetContentAsString(V8Engine engine, params InternalHandle[] args)
        {
            var encoding = Encoding.UTF8;
            if (args.Length > 0)
            {
                if (args[0].IsString == false)
                    throw new InvalidOperationException($"Encoding parameter must be of type string and convertible to one of the .NET supported encodings, but was '{args[0]}'.");

                var encodingAsString = args[0].AsString;
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

            return engine.CreateValue(_attachment.GetContentAsString(encoding));
        }

        public InternalHandle NamedPropertyGetter(V8Engine engine, ref string propertyName)
        {
            if (_properties.TryGetValue(propertyName, out InternalHandle value) == false)
            {
                if (propertyName == nameof(IAttachmentObject.Name))
                    value = engine.CreateValue(_attachment.Name);
                else if (propertyName == nameof(IAttachmentObject.ContentType))
                    value = engine.CreateValue(_attachment.ContentType);
                else if (propertyName == nameof(IAttachmentObject.Hash))
                    value = engine.CreateValue(_attachment.Hash);
                else if (propertyName == nameof(IAttachmentObject.Size))
                    value = engine.CreateValue(_attachment.Size);
                else if (propertyName == GetContentAsStringMethodName)
                    value = new ClrFunctionInstance(engine, GetContentAsStringMethodName, GetContentAsString);

                if (value.IsEmpty() == false)
                    _properties[propertyName].Set(value);
            }

            if (value.IsEmpty())
                value.Set((v8EngineEx)engine.CreateObjectBinder(DynamicJsNull.ImplicitNull)._);

            return value;
        }

        private InternalHandle GetContentAsString(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            var attachment = (AttachmentObjectInstance)self.BoundObject;
            if (bo == null)
                throw new InvalidOperationException($"GetContentAsString: BoundObject is null.");
            return attachment.GetContentAsString(engine, args);
        }

        public class CustomBinder : PropertiesObjectInstance.CustomBinder<AttachmentObjectInstance>
        {
            public override InternalHandle NamedPropertyGetter(ref string propertyName)
            {
                return _Handle.NamedPropertyGetter(Engine, propertyName);
            }
        }

    }
}
