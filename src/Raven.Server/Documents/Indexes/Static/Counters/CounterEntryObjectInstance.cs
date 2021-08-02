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

        public CounterEntryObjectInstance(DynamicCounterEntry entry, JavaScriptUtils javaScriptUtils = null) : base()
        {
            _entry = entry ?? throw new ArgumentNullException(nameof(entry));
        }

        public static CounterEntryObjectInstance.CustomBinder CreateObjectBinder(V8EngineEx engine, CounterEntryObjectInstance bo) {
            return engine.CreateObjectBinder<CounterEntryObjectInstance.CustomBinder>(bo, engine.TypeBinderCounterEntryObjectInstance);
        }

        public override InternalHandle NamedPropertyGetter(V8EngineEx engine, ref string propertyName)
        {
            if (_properties.TryGetValue(propertyName, out InternalHandle jsValue) == false)
            {
                jsValue = GetPropertyValue(engine, ref propertyName);
                if (jsValue.IsEmpty == false)
                    _properties.Add(propertyName, jsValue);
            }

            return jsValue;
        }

        private InternalHandle GetPropertyValue(V8EngineEx engine, ref string propertyName)
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
        }
    }
}
