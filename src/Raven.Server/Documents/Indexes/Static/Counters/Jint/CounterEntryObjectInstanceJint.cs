using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;

namespace Raven.Server.Documents.Indexes.Static.Counters.Jint
{
    public class CounterEntryObjectInstanceJint : ObjectInstance, IObjectInstance
    {
        private JintEngineEx _engineEx;
        private Engine _engine;

        private readonly DynamicCounterEntry _entry;

        private Dictionary<JsValue, PropertyDescriptor> _properties = new Dictionary<JsValue, PropertyDescriptor>();
        
        public IJsEngineHandle EngineHandle => _engineEx;

        public JsHandle CreateJsHandle(bool keepAlive = false)
        {
            return new JsHandle(this);
        }

        public void Dispose()
        {}

        public CounterEntryObjectInstanceJint(JintEngineEx engineEx, DynamicCounterEntry entry)
            : base(engineEx)
        {
            _engineEx = engineEx;
            _engine = _engineEx;
                
            _entry = entry ?? throw new ArgumentNullException(nameof(entry));

            SetPrototypeOf(_engine.Realm.Intrinsics.Object.PrototypeObject);
        }

        public override bool Delete(JsValue property)
        {
            throw new NotSupportedException();
        }

        public override PropertyDescriptor GetOwnProperty(JsValue property)
        {
            if (_properties.TryGetValue(property, out var value) == false)
                _properties[property] = value = GetPropertyValue(property);

            return value;
        }

        private PropertyDescriptor GetPropertyValue(JsValue property)
        {
            if (property == nameof(DynamicCounterEntry.Value))
                return new PropertyDescriptor(_entry._value, writable: false, enumerable: false, configurable: false);

            if (property == nameof(DynamicCounterEntry.DocumentId))
                return new PropertyDescriptor(_entry._counterItemMetadata.DocumentId.ToString(), writable: false, enumerable: false, configurable: false);

            if (property == nameof(DynamicCounterEntry.Name))
                return new PropertyDescriptor(_entry._counterItemMetadata.CounterName.ToString(), writable: false, enumerable: false, configurable: false);

            return PropertyDescriptor.Undefined;
        }

        public override bool Set(JsValue property, JsValue value, JsValue receiver)
        {
            throw new NotSupportedException();
        }

        public override IEnumerable<KeyValuePair<JsValue, PropertyDescriptor>> GetOwnProperties()
        {
            throw new NotSupportedException();
        }

        public override List<JsValue> GetOwnPropertyKeys(Types types = Types.String | Types.Symbol)
        {
            throw new NotSupportedException();
        }
    }
}
