using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Descriptors;

namespace Raven.Server.Documents.Indexes.Static.Counters
{
    public sealed class CounterEntryObjectInstance : ObjectInstance
    {
        private readonly DynamicCounterEntry _entry;

        private readonly Dictionary<string, PropertyDescriptor> _properties = new();

        public CounterEntryObjectInstance(Engine engine, DynamicCounterEntry entry)
            : base(engine)
        {
            ArgumentNullException.ThrowIfNull(entry);
            _entry = entry;
        }

        public override bool Delete(JsValue property)
        {
            throw new NotSupportedException();
        }

        public override PropertyDescriptor GetOwnProperty(JsValue property)
        {
            var key = property.ToString();
            if (_properties.TryGetValue(key, out var value) == false)
                _properties[key] = value = GetPropertyValue(key);

            return value;
        }

        private PropertyDescriptor GetPropertyValue(string property)
        {
            return property switch
            {
                nameof(DynamicCounterEntry.Value) => new PropertyDescriptor(_entry._value, writable: false, enumerable: false, configurable: false),
                nameof(DynamicCounterEntry.DocumentId) => new PropertyDescriptor(new LazyJsString(_entry._counterItemMetadata.DocumentId), writable: false, enumerable: false, configurable: false),
                nameof(DynamicCounterEntry.Name) => new PropertyDescriptor(new LazyJsString(_entry._counterItemMetadata.CounterName), writable: false, enumerable: false, configurable: false),
                _ => PropertyDescriptor.Undefined
            };
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
