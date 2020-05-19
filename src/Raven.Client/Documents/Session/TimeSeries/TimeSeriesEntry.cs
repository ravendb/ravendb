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
        public readonly byte Index;
        public TimeSeriesValueAttribute(byte index)
        {
            Index = index;
        }
    }

    public abstract class TimeSeriesEntryValues : IPostJsonDeserialization
    {
        public double[] Values { get; set; }

        private static readonly ConcurrentDictionary<Type, SortedDictionary<byte, MemberInfo>> _cache = new ConcurrentDictionary<Type, SortedDictionary<byte, MemberInfo>>();

        internal void SetValuesFromMembers()
        {
            var t = GetType();
            var mapping = GetMembersMapping(t);
            if (mapping == null)
                return;

            Values = new double[mapping.Count];
            foreach (var memberInfo in mapping)
            {
                var index = memberInfo.Key;
                var member = memberInfo.Value;
                Values[index] = (double)member.GetValue(this);
            }
        }

        internal void SetMembersFromValues()
        {
            var t = GetType();
            var mapping = GetMembersMapping(t);
            if (mapping == null)
                return;

            foreach (var memberInfo in mapping)
            {
                var index = memberInfo.Key;
                var member = memberInfo.Value;
                member.SetValue(this, Values[index]);
            }
        }

        internal static SortedDictionary<byte, MemberInfo> GetMembersMapping(Type type)
        {
            return _cache.GetOrAdd(type, (t) =>
            {
                SortedDictionary<byte, MemberInfo> mapping = null;
                foreach (var member in ReflectionUtil.GetPropertiesAndFieldsFor(t, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
                {
                    var attribute = member.GetCustomAttribute<TimeSeriesValueAttribute>(inherit: false);
                    if (attribute == null)
                        continue;

                    var i = attribute.Index;
                    mapping ??= new SortedDictionary<byte, MemberInfo>();
                    if (mapping.ContainsKey(i))
                        throw new InvalidOperationException($"Cannot map '{member.Name}' to {i}, since '{mapping[i].Name}' already mapped to it.");

                    mapping[i] = member;
                }

                if (mapping == null)
                    return null;

                if (mapping.Count == mapping.Keys.Last())
                    throw new InvalidOperationException($"The mapping of '{t}' must contain consecutive values starting from 0.");

                return mapping;
            });
        }

        [OnDeserialized]
        internal void OnNewtonSoftJsonDeserialized(StreamingContext context)
        {
            SetMembersFromValues();
        }

        void IPostJsonDeserialization.PostDeserialization()
        {
            SetMembersFromValues();
        }
    }
}
