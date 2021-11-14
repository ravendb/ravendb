using System;
using System.Collections.Generic;
using V8.Net;
using Raven.Client.Documents.Operations.Attachments;
using Sparrow.Json;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Extensions.V8;

namespace Raven.Server.Documents.Indexes.Static.JavaScript.V8
{
    public class AttachmentNameObjectInstanceV8 : ObjectInstanceBaseV8
    {
        private readonly BlittableJsonReaderObject _attachmentName;

        public override InternalHandle CreateObjectBinder(bool keepAlive = false)
        {
            var jsBinder = _engine.CreateObjectBinder<AttachmentNameObjectInstanceV8.CustomBinder>(this, EngineEx.Context.TypeBinderAttachmentNameObjectInstance(), keepAlive: keepAlive);
            var binder = (ObjectBinder)jsBinder.Object;
            binder.ShouldDisposeBoundObject = true;
            return jsBinder;
        }

        public AttachmentNameObjectInstanceV8(V8EngineEx engineEx, BlittableJsonReaderObject attachmentName) : base(engineEx)
        {
            _attachmentName = attachmentName ?? throw new ArgumentNullException(nameof(attachmentName));
        }

        public override InternalHandle NamedPropertyGetterOnce(V8EngineEx engineEx, ref string propertyName)
        {
            var engine = (V8Engine)engineEx;
            if (propertyName == nameof(AttachmentName.Name) && _attachmentName.TryGet(nameof(AttachmentName.Name), out string name))
                return engine.CreateValue(name);
            
            if (propertyName == nameof(AttachmentName.ContentType) && _attachmentName.TryGet(nameof(AttachmentName.ContentType), out string contentType))
                return engine.CreateValue(contentType);
            
            if (propertyName == nameof(AttachmentName.Hash) && _attachmentName.TryGet(nameof(AttachmentName.Hash), out string hash))
                return engine.CreateValue(hash);
            
            if (propertyName == nameof(AttachmentName.Size) && _attachmentName.TryGet(nameof(AttachmentName.Size), out long size))
                return engine.CreateValue(size);

            return InternalHandle.Empty;
        }

        public class CustomBinder : ObjectInstanceBaseV8.CustomBinder<AttachmentNameObjectInstanceV8>
        {
            public CustomBinder() : base()
            {}
        }
    }
}
