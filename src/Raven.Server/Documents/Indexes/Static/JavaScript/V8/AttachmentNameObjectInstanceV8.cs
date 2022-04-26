using System;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.Patch.V8;
using Sparrow.Json;
using V8.Net;

namespace Raven.Server.Documents.Indexes.Static.JavaScript.V8
{
    public class AttachmentNameObjectInstanceV8 : ObjectInstanceBaseV8
    {
        private readonly BlittableJsonReaderObject _attachmentName;

        public override InternalHandle CreateObjectBinder(bool keepAlive = false)
        {
            var jsBinder = _engine.CreateObjectBinder<CustomBinder<AttachmentNameObjectInstanceV8>>(this, EngineEx.Context.TypeBinderAttachmentNameObjectInstance(), keepAlive: keepAlive);
            var binder = (ObjectBinder)jsBinder.Object;
            binder.ShouldDisposeBoundObject = true;
            return jsBinder;
        }

        public AttachmentNameObjectInstanceV8(V8EngineEx engineEx, BlittableJsonReaderObject attachmentName) : base(engineEx)
        {
            _attachmentName = attachmentName ?? throw new ArgumentNullException(nameof(attachmentName));
        }

        public override InternalHandle NamedPropertyGetterOnce(ref string propertyName)
        {
            if (propertyName == nameof(AttachmentName.Name) && _attachmentName.TryGet(nameof(AttachmentName.Name), out string name))
                return EngineEx.CreateValue(name).Item;
            
            if (propertyName == nameof(AttachmentName.ContentType) && _attachmentName.TryGet(nameof(AttachmentName.ContentType), out string contentType))
                return EngineEx.CreateValue(contentType).Item;
            
            if (propertyName == nameof(AttachmentName.Hash) && _attachmentName.TryGet(nameof(AttachmentName.Hash), out string hash))
                return EngineEx.CreateValue(hash).Item;
            
            if (propertyName == nameof(AttachmentName.Size) && _attachmentName.TryGet(nameof(AttachmentName.Size), out long size))
                return EngineEx.CreateValue(size).Item;

            return InternalHandle.Empty;
        }
    }
}
