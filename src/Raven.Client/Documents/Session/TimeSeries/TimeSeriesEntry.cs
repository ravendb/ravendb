//-----------------------------------------------------------------------
// <copyright file="TimeSeriesValue.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Serialization;
using Raven.Client.Extensions;
using Raven.Client.Util;
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

    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public class TimeSeriesValueAttribute : Attribute
    {
        public readonly int Index;
        public TimeSeriesValueAttribute(int index)
        {
            Index = index;
        }
    }

    public abstract class TimeSeriesEntryValues : IPostJsonDeserialization
    {
        public double[] Values { get; set; }

        private static readonly ConcurrentDictionary<Type, SortedDictionary<int, MemberInfo>> _cache = new ConcurrentDictionary<Type, SortedDictionary<int, MemberInfo>>();

        internal void SetValuesFromFields()
        {
            var t = GetType();
            var mapping = GetFieldsMapping(t);
            if (mapping == null)
                return;

            Values = new double[mapping.Count];
            foreach (var fieldInfo in mapping)
            {
                var index = fieldInfo.Key;
                var field = fieldInfo.Value;
                Values[index] = (double)field.GetValue(this);
            }
        }

        internal void SetFieldsFromValues()
        {
            var t = GetType();
            var mapping = GetFieldsMapping(t);
            if (mapping == null)
                return;

            foreach (var memberInfo in mapping)
            {
                var index = memberInfo.Key;
                var field = memberInfo.Value;
                field.SetValue(this, Values[index]);
            }
        }

        internal static SortedDictionary<int, MemberInfo> GetFieldsMapping(Type type)
        {
            return _cache.GetOrAdd(type, (t) =>
            {
                SortedDictionary<int, MemberInfo> mapping = null;
                foreach (var field in ReflectionUtil.GetPropertiesAndFieldsFor(t, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
                {
                    var attribute = field.GetCustomAttribute<TimeSeriesValueAttribute>(inherit: false);
                    if (attribute == null)
                        continue;

                    var i = attribute.Index;
                    mapping ??= new SortedDictionary<int, MemberInfo>();
                    mapping[i] = field;
                }

                return mapping;
            });
        }

        [OnDeserialized]
        internal void OnNewtonSoftJsonDeserialized(StreamingContext context)
        {
            SetFieldsFromValues();
        }

        void IPostJsonDeserialization.PostDeserialization()
        {
            SetFieldsFromValues();
        }
    }
}
