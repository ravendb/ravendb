using System;
using System.Collections.Generic;
using V8.Net;
using Raven.Client.Documents.Operations.Attachments;
using Sparrow.Json;
using Raven.Server.Documents.Patch;

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

        public override InternalHandle NamedPropertyGetter(V8EngineEx engine, ref string propertyName)
        {
            if (_properties.TryGetValue(propertyName, out InternalHandle value) == false)
            {
                if (propertyName == nameof(AttachmentName.Name) && _attachmentName.TryGet(nameof(AttachmentName.Name), out string name))
                    value = engine.CreateValue(name);
                else if (propertyName == nameof(AttachmentName.ContentType) && _attachmentName.TryGet(nameof(AttachmentName.ContentType), out string contentType))
                    value = engine.CreateValue(contentType);
                else if (propertyName == nameof(AttachmentName.Hash) && _attachmentName.TryGet(nameof(AttachmentName.Hash), out string hash))
                    value = engine.CreateValue(hash);
                else if (propertyName == nameof(AttachmentName.Size) && _attachmentName.TryGet(nameof(AttachmentName.Size), out long size))
                    value = engine.CreateValue(size);

                if (value.IsEmpty == false)
                    _properties.Add(propertyName, value);
            }

            if (value.IsEmpty)
                value.Set(DynamicJsNull.ImplicitNull._);

            return value;
        }

        public class CustomBinder : ObjectInstanceBase.CustomBinder<AttachmentNameObjectInstance>
        {
            public CustomBinder() : base()
            {}
        }
    }
}
