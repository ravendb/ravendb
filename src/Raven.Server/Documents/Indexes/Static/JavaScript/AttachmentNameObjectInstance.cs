﻿using System;
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

        public override InternalHandle NamedPropertyGetter(V8EngineEx engine, ref string propertyName)
        {
            if (_properties.TryGetValue(propertyName, out InternalHandle jsValue) == false)
            {
                if (propertyName == nameof(AttachmentName.Name) && _attachmentName.TryGet(nameof(AttachmentName.Name), out string name))
                    jsValue = engine.CreateValue(name);
                else if (propertyName == nameof(AttachmentName.ContentType) && _attachmentName.TryGet(nameof(AttachmentName.ContentType), out string contentType))
                    jsValue = engine.CreateValue(contentType);
                else if (propertyName == nameof(AttachmentName.Hash) && _attachmentName.TryGet(nameof(AttachmentName.Hash), out string hash))
                    jsValue = engine.CreateValue(hash);
                else if (propertyName == nameof(AttachmentName.Size) && _attachmentName.TryGet(nameof(AttachmentName.Size), out long size))
                    jsValue = engine.CreateValue(size);

                if (jsValue.IsEmpty == false)
                    _properties.Add(propertyName, jsValue);
            }

            if (jsValue.IsEmpty)
                jsValue.Set(DynamicJsNull.ImplicitNull._);

            return jsValue;
        }

        public class CustomBinder : ObjectInstanceBase.CustomBinder<AttachmentNameObjectInstance>
        {
            public CustomBinder() : base()
            {}
        }
    }
}
