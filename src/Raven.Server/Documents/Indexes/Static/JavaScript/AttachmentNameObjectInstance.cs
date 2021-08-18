using System;
using System.Collections.Generic;
using V8.Net;
using Raven.Client.Documents.Operations.Attachments;
using Sparrow.Json;
using Raven.Server.Documents.Patch;
using Raven.Server.Extensions;

namespace Raven.Server.Documents.Indexes.Static.JavaScript
{
    //[ScriptObject("AttachmentNameObjectInstance", ScriptMemberSecurity.NoAcccess)]
    public class AttachmentNameObjectInstance : ObjectInstanceBase
    {
        private readonly BlittableJsonReaderObject _attachmentName;

        public AttachmentNameObjectInstance(BlittableJsonReaderObject attachmentName) : base()
        {
            _attachmentName = attachmentName ?? throw new ArgumentNullException(nameof(attachmentName));
        }

        public override InternalHandle NamedPropertyGetterOnce(V8EngineEx engine, ref string propertyName)
        {
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

        public class CustomBinder : ObjectInstanceBase.CustomBinder<AttachmentNameObjectInstance>
        {
            public CustomBinder() : base()
            {}
        }
    }
}
