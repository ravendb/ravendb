using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Attachments;
using Sparrow.Json;

using V8.Net;

using V8.Net;

namespace Raven.Server.Documents.Indexes.Static.JavaScript.V8
{
    //[ScriptObject("AttachmentNameObjectInstance", ScriptMemberSecurity.NoAcccess)]
    public class AttachmentNameObjectInstance : PropertiesObjectInstance
    {
        private readonly BlittableJsonReaderObject _attachmentName;

        public AttachmentNameObjectInstance() : base()
        {
            assert(false);
        }
        
        public AttachmentNameObjectInstance(BlittableJsonReaderObject attachmentName) : base()
        {
            _attachmentName = attachmentName ?? throw new ArgumentNullException(nameof(attachmentName));
        }

        public InternalHandle NamedPropertyGetter(V8Engine engine, ref string propertyName)
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

                if (value.IsEmpty() == false)
                    _properties[propertyName].Set(value);
            }

            if (value.IsEmpty())
                value.Set((v8EngineEx)engine.CreateObjectBinder(DynamicJsNull.ImplicitNull)._);

            return value;
        }

        public class CustomBinder : PropertiesObjectInstance.CustomBinder<AttachmentNameObjectInstance>
        {
            public override InternalHandle NamedPropertyGetter(ref string propertyName)
            {
                return _Handle.NamedPropertyGetter(Engine, propertyName);
            }
        }
    }
}
