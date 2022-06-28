using System;
using System.Text;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Patch.V8;
using V8.Net;

namespace Raven.Server.Documents.Indexes.Static.JavaScript.V8
{
    public class AttachmentObjectInstanceV8 : ObjectInstanceBaseV8
    {
        private const string GetContentAsStringMethodName = "getContentAsString";

        private readonly DynamicAttachment _attachment;

        public override InternalHandle CreateObjectBinder(bool keepAlive = false)
        {
            var jsBinder = _engine.CreateObjectBinder<CustomBinder<AttachmentObjectInstanceV8>>(this, EngineEx.Context.TypeBinderAttachmentObjectInstance(), keepAlive: keepAlive);
            var binder = (ObjectBinder)jsBinder.Object;
            binder.ShouldDisposeBoundObject = true;
            return jsBinder;
        }

        public AttachmentObjectInstanceV8(V8EngineEx engineEx, DynamicAttachment attachment) : base(engineEx)
        {
            _attachment = attachment ?? throw new ArgumentNullException(nameof(attachment));
        }

        public override InternalHandle NamedPropertyGetterOnce(ref string propertyName)
        {
            if (propertyName == nameof(IAttachmentObject.Name))
                return EngineEx.CreateValue(_attachment.Name).Item;
            
            if (propertyName == nameof(IAttachmentObject.ContentType))
                return EngineEx.CreateValue(_attachment.ContentType).Item;
            
            if (propertyName == nameof(IAttachmentObject.Hash))
                return EngineEx.CreateValue(_attachment.Hash).Item;
            
            if (propertyName == nameof(IAttachmentObject.Size))
                return EngineEx.CreateValue(_attachment.Size).Item;
            
            if (propertyName == GetContentAsStringMethodName)
                return EngineEx.CreateClrCallBack(GetContentAsStringMethodName, GetContentAsString, keepAlive: false).Item;

            return InternalHandle.Empty;
        }

        private JsHandleV8 GetContentAsString(JsHandleV8 self, JsHandleV8[] args) // callback
        {
            try
            {
                if (self.IsObject && self.AsObject() is AttachmentObjectInstanceV8 attachment)
                {
                    if (attachment == null)
                        throw new InvalidOperationException($"GetContentAsString: BoundObject is null.");


                    return attachment.GetContentAsStringInternal(args);
                }
                else
                {
                    return EngineEx.Empty;
                }
            }
            catch (Exception e)
            {
                //  EngineEx.Context.JsContext.LastException = e;
              //  var str = engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                return EngineEx.CreateError(e, JSValueType.ExecutionError);
            }
        }

        private JsHandleV8 GetContentAsStringInternal(JsHandleV8[] args)
        {
            var encoding = Encoding.UTF8;
            if (args.Length > 0)
            {
                if (args[0].IsStringEx == false)
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
            return EngineEx.CreateValue(_attachment.GetContentAsString(encoding));
        }
    }
}
