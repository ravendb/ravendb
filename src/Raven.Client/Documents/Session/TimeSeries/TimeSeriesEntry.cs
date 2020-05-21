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
using Newtonsoft.Json;
using Raven.Client.Documents.Queries.TimeSeries;
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
            get
            {
                if (Values.Length == 1)
                    return Values[0];

                throw new InvalidOperationException("Entry has more than one value.");
            }
            set
            {
                if (Values.Length == 1)
                {
                    Values[0] = value;
                    return;
                }

                throw new InvalidOperationException("Entry has more than one value.");
            }
        }

        public TimeSeriesRollupEntry<T> ToRollupEntry<T>() where T : TimeSeriesEntry, new()
        {
            if (IsRollup == false)
                throw new InvalidCastException("Not a rolled up entry.");

            if (typeof(T) != GetType())
                throw new InvalidCastException($"Can't cast '{typeof(T).FullName}' to '{GetType().FullName}'");

            return new TimeSeriesRollupEntry<T>
            {
                Tag = Tag,
                Timestamp = Timestamp,
                Values = Values,
                IsRollup = true
            };
        }
    }

    public static class RollupExtensions
    {
        public static TimeSeriesRollupEntry<T> AsRollUpEntry<T>(this TimeSeriesEntry entry) where T : TimeSeriesEntry, new()
        {
            if (entry.IsRollup == false)
                throw new InvalidCastException("Not a rolled up entry.");

            if (entry is TimeSeriesRollupEntry<T> rollupEntry)
                return rollupEntry;

            if (typeof(T) != entry.GetType())
                throw new InvalidCastException($"Can't cast '{typeof(T).FullName}' to '{entry.GetType().FullName}'");

            return new TimeSeriesRollupEntry<T>
            {
                Tag = entry.Tag,
                Timestamp = entry.Timestamp,
                Values = entry.Values,
                IsRollup = true
            };
        }
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
        public bool IsRollup { get; set; }

        private static readonly ConcurrentDictionary<Type, SortedDictionary<byte, MemberInfo>> _cache = new ConcurrentDictionary<Type, SortedDictionary<byte, MemberInfo>>();

        internal virtual void SetValuesFromMembers()
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
                if (IsRollup)
                    index *= 6;

                Values[index] = (double)member.GetValue(this);
            }
        }

        internal virtual void SetMembersFromValues()
        {
            var t = GetType();
            var mapping = GetMembersMapping(t);
            if (mapping == null)
                return;

            foreach (var memberInfo in mapping)
            {
                var index = memberInfo.Key;
                var member = memberInfo.Value;
                if (IsRollup)
                    index *= 6;
                
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

    public class TimeSeriesRollupEntry<TTimeSeriesEntry> : TimeSeriesEntry, IPostJsonDeserialization where TTimeSeriesEntry : TimeSeriesEntry, new()
    {
        private int _dim;

        private TTimeSeriesEntry _first;
        private TTimeSeriesEntry _last;
        private TTimeSeriesEntry _max;
        private TTimeSeriesEntry _min;
        private TTimeSeriesEntry _sum;
        private TTimeSeriesEntry _count;

        private TTimeSeriesEntry _average;

        internal TimeSeriesRollupEntry(){ /* for de-serialization*/ }

        public TimeSeriesRollupEntry(DateTime timestamp)
        {
            IsRollup = true;
            var map = GetMembersMapping(typeof(TTimeSeriesEntry));
            Values = new double[map.Count * 6];
            Timestamp = timestamp;
        }

        [JsonIgnore]
        public TTimeSeriesEntry First
        {
            get
            {
                if (_first?.Values != null)
                    return _first;

                Build2DArray();
                _first = new TTimeSeriesEntry
                {
                    Timestamp = Timestamp,
                    Values = _innerValues[(int)AggregationType.First]
                };
                _first.SetMembersFromValues();
                return _first;
            }
        }

        [JsonIgnore]
        public TTimeSeriesEntry Last
        {
            get
            {
                if (_last?.Values != null)
                    return _last;

                Build2DArray();
                _last = new TTimeSeriesEntry
                {
                    Timestamp = Timestamp, 
                    Values = _innerValues[(int)AggregationType.Last]
                };
                _last.SetMembersFromValues();
                return _last;
            }
        }

        [JsonIgnore]
        public TTimeSeriesEntry Min
        {
            get
            {
                if (_min?.Values != null)
                    return _min;

                Build2DArray();
                _min = new TTimeSeriesEntry
                {
                    Timestamp = Timestamp, 
                    Values = _innerValues[(int)AggregationType.Min]
                };
                _min.SetMembersFromValues();
                return _min;
            }
        }

        [JsonIgnore]
        public TTimeSeriesEntry Max
        {
            get
            {
                if (_max?.Values != null)
                    return _max;

                Build2DArray();
                _max = new TTimeSeriesEntry
                {
                    Timestamp = Timestamp, 
                    Values = _innerValues[(int)AggregationType.Max]
                };
                _max.SetMembersFromValues();
                return _max;
            }
        }

        [JsonIgnore]
        public TTimeSeriesEntry Sum
        {
            get
            {
                if (_sum?.Values != null)
                    return _sum;

                Build2DArray();
                _sum = new TTimeSeriesEntry
                {
                    Timestamp = Timestamp, 
                    Values = _innerValues[(int)AggregationType.Sum]
                };
                _sum.SetMembersFromValues();
                return _sum;
            }
        }

        [JsonIgnore]
        public TTimeSeriesEntry Count
        {
            get
            {
                if (_count?.Values != null)
                    return _count;

                Build2DArray();
                _count = new TTimeSeriesEntry
                {
                    Timestamp = Timestamp,
                    Values = _innerValues[(int)AggregationType.Count]
                };
                _count.SetMembersFromValues();
                return _count;
            }
        }

        [JsonIgnore]
        public TTimeSeriesEntry Average
        {
            get
            {
                if (_average?.Values != null)
                    return _average;

                Build2DArray();
                var arr = new double[_dim];
                for (int i = 0; i < arr.Length; i++)
                {
                    arr[i] = _innerValues[(int)AggregationType.Sum][i] / _innerValues[(int)AggregationType.Count][i];
                }

                _average = new TTimeSeriesEntry
                {
                    Timestamp = Timestamp,
                    Values = arr
                };
                _average.SetMembersFromValues();
                return _average;
            }
        }

        private bool _innerArrayInitialized;
        private void Build2DArray()
        {
            if (IsRollup == false)
                throw new InvalidOperationException("Not a rolled up entry.");

            if (_innerArrayInitialized)
                return;

            _dim = Values.Length / 6;
            _innerValues = new double[6][];
            for (int i = 0; i < 6; i++)
            {
                _innerValues[i] = new double[_dim];
                for (int j = 0; j < _dim; j++)
                {
                    _innerValues[i][j] = Values[j * 6 + i];
                }
            }

            _innerArrayInitialized = true;
        }

        private double[][] _innerValues;

        [OnDeserialized]
        internal new void OnNewtonSoftJsonDeserialized(StreamingContext context)
        {
            Build2DArray();
        }

        void IPostJsonDeserialization.PostDeserialization()
        {
            Build2DArray();
        }

        internal override void SetValuesFromMembers()
        {
            SetInternal(_first, (int)AggregationType.First);
            SetInternal(_last, (int)AggregationType.Last);
            SetInternal(_min, (int)AggregationType.Min);
            SetInternal(_max, (int)AggregationType.Max);
            SetInternal(_count, (int)AggregationType.Count);
            SetInternal(_sum, (int)AggregationType.Sum);
        }

        private void SetInternal(TTimeSeriesEntry entry, int position)
        {
            if (entry == null) 
                return;

            entry.SetValuesFromMembers();
            for (int i = 0; i < entry.Values.Length; i++)
            {
                Values[6 * i + position] = entry.Values[i];
            }
        }

        public static explicit operator TTimeSeriesEntry(TimeSeriesRollupEntry<TTimeSeriesEntry> rollupEntry)
        {
            return rollupEntry.First;
        }
    }
}
