using System;
using System.Collections;
using System.Collections.Generic;

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

    public interface IPropertyAccessor
    {
        IEnumerator<PropertyItem> GetProperties(object target);

        object GetValue(string name, object target);
    }
}
