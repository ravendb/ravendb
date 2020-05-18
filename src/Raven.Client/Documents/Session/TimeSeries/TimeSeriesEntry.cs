//-----------------------------------------------------------------------
// <copyright file="TimeSeriesValue.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.TimeSeries
{
    public class TimeSeriesEntry : TimeSeriesEntryValues
    {
        public DateTime Timestamp { get; set; }

        public string Tag { get; set; }

        [JsonDeserializationIgnore]
        public double Value
        {
            get => Values[0]; 
            set => Values[0] = value;
        }
    }

    public abstract class TimeSeriesAggregatedEntry : TimeSeriesEntryValues
    {

    }

    [AttributeUsage(AttributeTargets.Field)]
    public class TimeSeriesValue : Attribute
    {
        public int Index;
        public TimeSeriesValue(int index)
        {
            Index = index;
        }
    }

    public abstract class TimeSeriesEntryValues : IPostDeserialization
    {
        public double[] Values { get; set; }

        private static readonly ConcurrentDictionary<Type, TimeSeriesEntryMappingInfo> _cache = new ConcurrentDictionary<Type, TimeSeriesEntryMappingInfo>();

        private class TimeSeriesEntryMappingInfo
        {
            public readonly SortedDictionary<int, FieldInfo> FieldsMapping = new SortedDictionary<int, FieldInfo>();
        }

        internal void SetValuesFromFields()
        {
            var t = GetType();
            var mapping = GetFieldsMapping(t);
            if (mapping == null)
                return;

            var fields = mapping.FieldsMapping;
            Values = new double[fields.Count];
            foreach (var fieldInfo in fields)
            {
                var index = fieldInfo.Key;
                var field = fieldInfo.Value;
                Values[index] = (double)field.GetValue(this);
            }
        }

        private static TimeSeriesEntryMappingInfo GetFieldsMapping(Type t)
        {
            if (_cache.TryGetValue(t, out var mapping) == false)
            {
                foreach (var field in t.GetFields())
                {
                    var attribute = (TimeSeriesValue)field.GetCustomAttributes(false).SingleOrDefault(a => a is TimeSeriesValue);
                    if (attribute == null)
                        continue;

                    var i = attribute.Index;
                    mapping ??= new TimeSeriesEntryMappingInfo();
                    mapping.FieldsMapping[i] = field;
                }

                mapping = _cache.GetOrAdd(t, mapping);
            }

            return mapping;
        }

        internal void SetFieldsFromValues()
        {
            var t = GetType();
            var mapping = GetFieldsMapping(t);
            if (mapping == null)
                return;

            foreach (var fieldInfo in mapping.FieldsMapping)
            {
                var index = fieldInfo.Key;
                var field = fieldInfo.Value;
                field.SetValue(this, Values[index]);
            }
        }

        [OnDeserialized]
        internal void OnNewtonSoftJsonDeserialized(StreamingContext context)
        {
            SetFieldsFromValues();
        }

        void IPostDeserialization.PostDeserialization()
        {
            SetFieldsFromValues();
        }
    }
}
