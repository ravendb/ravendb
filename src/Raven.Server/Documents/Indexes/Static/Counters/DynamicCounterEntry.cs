using System.Diagnostics;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes.Static.Counters
{
    public class DynamicCounterEntry : AbstractDynamicObject
    {
        private CounterEntry _counterEntry;
        private QueryOperationContext _context;
        private Index _index;

        private long _value;
        private bool _valueSet;

        public override dynamic GetId()
        {
            if (_counterEntry == null)
                return DynamicNullObject.Null;

            Debug.Assert(_counterEntry.DocumentId != null, "_counterEntry.DocumentId != null");

            return _counterEntry.DocumentId;
        }

        public override void Set(object item)
        {
            _counterEntry = (CounterEntry)item;
            _value = 0;
            _valueSet = false;

            if (_context == null)
            {
                var current = CurrentIndexingScope.Current;
                _context = current.QueryContext;
                _index = current.Index;
            }

            Debug.Assert(_context == CurrentIndexingScope.Current.QueryContext, "_context == CurrentIndexingScope.Current.QueryContext");
        }

        public dynamic DocumentId => TypeConverter.ToDynamicType(_counterEntry.DocumentId);

        public dynamic Value
        {
            get
            {
                if (_valueSet == false)
                {
                    Debug.Assert(_context != null, "_context != null");

                    var value = _index
                        .DocumentDatabase
                        .DocumentsStorage
                        .CountersStorage
                        .GetCounterValue(_context.Documents, _counterEntry.DocumentId, _counterEntry.Name);

                    if (value != null)
                        _value = value.Value.Value;

                    _valueSet = true;
                }

                return _value;
            }
        }

        protected override bool TryGetByName(string name, out object result)
        {
            Debug.Assert(_counterEntry != null, "Item cannot be null");

            result = DynamicNullObject.Null;
            return true;
        }
    }
}
