using System;
using System.Collections.Generic;
using V8.Net;

//using Raven.Server.Documents.Indexes.Static.JavaScript;

namespace Raven.Server.Documents.Indexes.Static.Counters
{
    public class CounterEntryObjectInstance : ObjectInstanceBase
    {
        private readonly DynamicCounterEntry _entry;

        private Dictionary<InternalHandle, PropertyDescriptor> _properties = new Dictionary<InternalHandle, PropertyDescriptor>();

        public CounterEntryObjectInstance(DynamicCounterEntry entry) : base()
        {
            _entry = entry ?? throw new ArgumentNullException(nameof(entry));
        }

        public CounterEntryObjectInstance() : base()
        {
            assert(false);
        }
        
        public InternalHandle NamedPropertyGetter(V8Engine engine, ref string propertyName)
        {
            if (_properties.TryGetValue(propertyName, out InternalHandle value) == false)
            {
                value = GetPropertyValue(engine, propertyName);
                _properties[propertyName].Set(value);
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
            public override InternalHandle NamedPropertyGetter(ref string propertyName)
            {
                return _Handle.NamedPropertyGetter(Engine, propertyName);
            }
        }
    }
}
