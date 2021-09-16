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

        public CounterEntryObjectInstance(DynamicCounterEntry entry, JavaScriptUtils javaScriptUtils = null) : base(false)
        {
            _entry = entry ?? throw new ArgumentNullException(nameof(entry));
        }

        public static InternalHandle CreateObjectBinder(V8EngineEx engine, CounterEntryObjectInstance bo, bool keepAlive = false) {
            return engine.CreateObjectBinder<CounterEntryObjectInstance.CustomBinder>(bo, engine.TypeBinderCounterEntryObjectInstance, keepAlive: keepAlive);
        }

        public override InternalHandle NamedPropertyGetterOnce(V8EngineEx engine, ref string propertyName)
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
