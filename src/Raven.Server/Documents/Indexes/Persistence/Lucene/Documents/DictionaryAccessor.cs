using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public sealed class DictionaryAccessor : IPropertyAccessor
    {
        private readonly Dictionary<string, DictionaryValueAccessor> _properties = new();

        private readonly List<KeyValuePair<string, DictionaryValueAccessor>> _propertiesInOrder = new();

        private DictionaryAccessor(Dictionary<string, object> instance, Dictionary<string, CompiledIndexField> groupByFields = null)
        {
            if (instance == null)
                throw new NotSupportedException("Indexed dictionary must be of type: Dictionary<string, object>");

            foreach (var key in instance.Keys)
            {
                var getMethod = new DictionaryValueAccessor(key);

                if (groupByFields != null)
                {
                    foreach (var groupByField in groupByFields.Values)
                    {
                        if (groupByField.IsMatch(key))
                        {
                            getMethod.GroupByField = groupByField;
                            getMethod.IsGroupByField = true;
                            break;
                        }
                    }
                }

                _properties.Add(key, getMethod);
                _propertiesInOrder.Add(new KeyValuePair<string, DictionaryValueAccessor>(key, getMethod));
            }
        }

        internal static DictionaryAccessor Create(Dictionary<string, object> instance, Dictionary<string, CompiledIndexField> groupByFields = null)
        {
            return new DictionaryAccessor(instance, groupByFields);
        }

        internal struct DictionaryPropertiesEnumerator : IEnumerator<PropertyItem>
        {
            private readonly List<KeyValuePair<string, DictionaryValueAccessor>> _propertiesInOrder;
            private readonly object _target;
            private int _currentIdx;

            internal DictionaryPropertiesEnumerator(List<KeyValuePair<string, DictionaryValueAccessor>> properties, object target)
            {
                _propertiesInOrder = properties;
                _target = target;
                _currentIdx = -1;
            }

            public bool MoveNext()
            {
                _currentIdx++;
                return _currentIdx < _propertiesInOrder.Count;
            }

            public void Reset()
            {
                _currentIdx = -1;
            }

            object IEnumerator.Current => Current;

            public PropertyItem Current
            {
                get
                {
                    var (key, value) = _propertiesInOrder[_currentIdx];
                    return new PropertyItem(key, value.GetValue(_target), value.GroupByField, value.IsGroupByField);
                }
            }

            public void Dispose() { }
        }


        public IEnumerator<PropertyItem> GetProperties(object target)
        {
            return new DictionaryPropertiesEnumerator(_propertiesInOrder, target);
        }

        public object GetValue(string name, object target)
        {
            if (_properties.TryGetValue(name, out DictionaryValueAccessor accessor))
                return accessor.GetValue(target);

            throw new InvalidOperationException($"The {name} property was not found");
        }

        internal sealed class DictionaryValueAccessor : PropertyAccessor.Accessor
        {
            private readonly string _propertyName;

            public DictionaryValueAccessor(string propertyName)
            {
                _propertyName = propertyName;
            }

            public override object GetValue(object target)
            {
                return ((Dictionary<string, object>)target)[_propertyName];
            }
        }
    }
}
