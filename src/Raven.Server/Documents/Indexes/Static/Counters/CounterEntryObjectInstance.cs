using System;
using System.Collections.Generic;
using V8.Net;
using Raven.Server.Documents.Indexes.Static.JavaScript;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Documents.Indexes.Static.Counters
{
    public class CounterEntryObjectInstance : ObjectInstanceBase
    {
        private readonly DynamicCounterEntry _entry;

        public CounterEntryObjectInstance(DynamicCounterEntry entry, JavaScriptUtils javaScriptUtils = null) : base(javaScriptUtils)
        {
            _entry = entry ?? throw new ArgumentNullException(nameof(entry));
        }

        public InternalHandle NamedPropertyGetter(V8Engine engine, ref string propertyName)
        {
            if (_properties.TryGetValue(propertyName, out InternalHandle value) == false)
            {
                value = GetPropertyValue(engine, propertyName);
                if (value.IsEmpty == false)
                    _properties.Add(propertyName, value);
            }

            return value;
        }

        private InternalHandle GetPropertyValue(V8Engine engine, ref string propertyName)
        {
            if (propertyName == nameof(DynamicCounterEntry.Value))
                return engine.CreateValue(_entry._value);

            if (propertyName == nameof(DynamicCounterEntry.DocumentId))
                return engine.CreateValue(_entry._counterItemMetadata.DocumentId.ToString());

            if (propertyName == nameof(DynamicCounterEntry.Name))
                return engine.CreateValue(_entry._counterItemMetadata.CounterName.ToString());

            return InternalHandle.Empty;
        }

        public class CustomBinder : ObjectInstanceBase.CustomBinder<CounterEntryObjectInstance>
        {
            public CustomBinder() : base()
            {}

            public override InternalHandle NamedPropertyGetter(ref string propertyName)
            {
                return objCLR.NamedPropertyGetter(Engine, ref propertyName);
            }
        }
    }
}
