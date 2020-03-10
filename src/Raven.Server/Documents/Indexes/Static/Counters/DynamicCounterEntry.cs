using System.Diagnostics;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static.Counters
{
    public class DynamicCounterEntry : AbstractDynamicObject
    {
        private CounterEntry _counterEntry;

        private long _value;

        public override dynamic GetId()
        {
            if (_counterEntry == null)
                return DynamicNullObject.Null;

            Debug.Assert(_counterEntry.DocumentId != null, "_counterEntry.DocumentId != null");

            return _counterEntry.DocumentId;
        }

        public override bool Set(object item)
        {
            _counterEntry = (CounterEntry)item;
            _value = 0;

            var current = CurrentIndexingScope.Current;

            var value = current.Index
                .DocumentDatabase
                .DocumentsStorage
                .CountersStorage
                .GetCounterValue(current.QueryContext.Documents, _counterEntry.DocumentId, _counterEntry.Name);

            if (value == null)
                return false;

            _value = value.Value.Value;
            return true;
        }

        public dynamic DocumentId => TypeConverter.ToDynamicType(_counterEntry.DocumentId);

        public LazyStringValue Name
        {
            get
            {
                return _counterEntry.Name;
            }
        }

        public dynamic Value => _value;

        protected override bool TryGetByName(string name, out object result)
        {
            Debug.Assert(_counterEntry != null, "Item cannot be null");

            result = DynamicNullObject.Null;
            return true;
        }
    }
}
