using System;
using System.Collections.Generic;
using V8.Net;
using Raven.Server.Documents.Indexes.Static.JavaScript.V8;
using Raven.Server.Documents.Patch.V8;

namespace Raven.Server.Documents.Indexes.Static.Counters.V8
{
    public class CounterEntryObjectInstanceV8 : ObjectInstanceBaseV8
    {
        private readonly DynamicCounterEntry _entry;

        public CounterEntryObjectInstanceV8(V8EngineEx engineEx, DynamicCounterEntry entry) 
            : base(engineEx, false)
        {
            _entry = entry ?? throw new ArgumentNullException(nameof(entry));
        }

        public override InternalHandle CreateObjectBinder(bool keepAlive = false)
        {
            var jsBinder = _engine.CreateObjectBinder<CounterEntryObjectInstanceV8.CustomBinder>(this, EngineEx.Context.TypeBinderCounterEntryObjectInstance(), keepAlive: keepAlive);
            var binder = (ObjectBinder)jsBinder.Object;
            binder.ShouldDisposeBoundObject = true;
            return jsBinder;
        }

        public override InternalHandle NamedPropertyGetterOnce(V8EngineEx engineEx, ref string propertyName)
        {
            var engine = (V8Engine)engineEx;
            if (propertyName == nameof(DynamicCounterEntry.Value))
                return engine.CreateValue(_entry._value);

            if (propertyName == nameof(DynamicCounterEntry.DocumentId))
                return engine.CreateValue(_entry._counterItemMetadata.DocumentId.ToString());

            if (propertyName == nameof(DynamicCounterEntry.Name))
                return engine.CreateValue(_entry._counterItemMetadata.CounterName.ToString());

            return InternalHandle.Empty;
        }

        public class CustomBinder : ObjectInstanceBaseV8.CustomBinder<CounterEntryObjectInstanceV8>
        {
            public CustomBinder() : base()
            {}
        }
    }
}
