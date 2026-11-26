using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public struct PropertyItem
    {
        public string Key;
        public object Value;
        public CompiledIndexField GroupByField;
        public bool IsGroupByField;

        public PropertyItem(string key, object value, CompiledIndexField groupByField, bool isGroupByField)
        {
            Key = key;
            Value = value;
            GroupByField = groupByField;
            IsGroupByField = isGroupByField;
        }
    }

    public struct PropertiesEnumerator : IEnumerator<PropertyItem>
    {
        private readonly List<KeyValuePair<string, PropertyAccessor.Accessor>> _propertiesInOrder;
        private readonly object _target;
        private int _currentIdx;

        private readonly ObjectInstance _objectInstance;
        private readonly Dictionary<string, CompiledIndexField> _groupByFields;
        private IEnumerator<KeyValuePair<JsValue, PropertyDescriptor>> _jsProperties;

        internal PropertiesEnumerator(List<KeyValuePair<string, PropertyAccessor.Accessor>> properties, object target)
        {
            _propertiesInOrder = properties;
            _target = target;
            _currentIdx = -1;
        }

        internal PropertiesEnumerator(ObjectInstance objectInstance, Dictionary<string, CompiledIndexField> groupByFields)
        {
            _objectInstance = objectInstance;
            _groupByFields = groupByFields;
            _jsProperties = objectInstance.GetOwnProperties().GetEnumerator();
        }

        public PropertiesEnumerator GetEnumerator() => this;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            if (_jsProperties != null)
                return _jsProperties.MoveNext();

            _currentIdx++;
            return _currentIdx < _propertiesInOrder.Count;
        }

        public PropertyItem Current
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (_jsProperties != null)
                    return JintPropertyAccessor.GetPropertyItem(_jsProperties.Current, _groupByFields);

                var (key, value) = _propertiesInOrder[_currentIdx];
                return new PropertyItem(key, value.GetValue(_target), value.GroupByField, value.IsGroupByField);
            }
        }

        object IEnumerator.Current => Current;

        public void Reset()
        {
            if (_objectInstance != null)
                _jsProperties = _objectInstance.GetOwnProperties().GetEnumerator();
            else
                _currentIdx = -1;
        }

        public void Dispose()
        {
        }
    }

    public interface IPropertyAccessor
    {
        PropertiesEnumerator GetProperties(object target);

        object GetValue(string name, object target);
    }
}
