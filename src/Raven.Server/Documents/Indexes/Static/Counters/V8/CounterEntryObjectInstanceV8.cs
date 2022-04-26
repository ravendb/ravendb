using System;
using Raven.Server.Documents.Indexes.Static.JavaScript.V8;
using Raven.Server.Documents.Patch.V8;
using V8.Net;

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
            var jsBinder = _engine.CreateObjectBinder<CustomBinder<CounterEntryObjectInstanceV8>>(this, EngineEx.Context.TypeBinderCounterEntryObjectInstance(), keepAlive: keepAlive);
            var binder = (ObjectBinder)jsBinder.Object;
            binder.ShouldDisposeBoundObject = true;
            return jsBinder;
        }

        public override InternalHandle NamedPropertyGetterOnce(ref string propertyName)
        {
            if (propertyName == nameof(DynamicCounterEntry.Value))
                return EngineEx.Engine.CreateValue(_entry._value);

            if (propertyName == nameof(DynamicCounterEntry.DocumentId))
                return EngineEx.Engine.CreateValue(_entry._counterItemMetadata.DocumentId.ToString());

            if (propertyName == nameof(DynamicCounterEntry.Name))
                return EngineEx.Engine.CreateValue(_entry._counterItemMetadata.CounterName.ToString());

            return InternalHandle.Empty;
        }
    }
}
