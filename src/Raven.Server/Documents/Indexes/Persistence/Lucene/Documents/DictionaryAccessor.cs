using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public class DictionaryAccessor : PropertyAccessor
    {
        private DictionaryAccessor(Dictionary<string, object> instance, HashSet<string> groupByFields = null) : base(instance.GetType(), groupByFields)
        {
            if (instance == null)
                throw new NotSupportedException("Indexed dictionary must be of type: Dictionary<string, object>");

            foreach (var key in instance.Keys)
            {
                var getMethod = new DictionaryValueAccessor(key);

                if (groupByFields != null && groupByFields.Contains(key))
                    getMethod.IsGroupByField = true;

                Properties.Add(key, getMethod);
                PropertiesInOrder.Add(new KeyValuePair<string, Accessor>(key, getMethod));
            }
        }

        internal static DictionaryAccessor Create(Dictionary<string, object> instance, HashSet<string> groupByFields = null)
        {
            return new DictionaryAccessor(instance, groupByFields);
        }

        private class DictionaryValueAccessor : Accessor
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