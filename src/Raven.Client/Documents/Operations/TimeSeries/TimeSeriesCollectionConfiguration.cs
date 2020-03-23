using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesCollectionConfiguration : IDynamicJson
    {
        public bool Disabled { get; set; }
        
        /// <summary>
        /// Specify roll up and retention policy.
        /// Each policy will create a new time-series aggregated from the previous one
        /// </summary>
        public List<TimeSeriesPolicy> Policies { get; set; } = new List<TimeSeriesPolicy>();

        /// <summary>
        /// Specify a policy for the original time-series
        /// </summary>
        public RawTimeSeriesPolicy RawPolicy { get; set; } = RawTimeSeriesPolicy.Default;

        internal void Initialize()
        {
            if (Policies.Count == 0)
                return;

            Policies.Sort(TimeSeriesDownSamplePolicyComparer.Instance);

            var hashSet = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
            for (var index = 0; index < Policies.Count; index++)
            {
                var policy = Policies[index];
                if (hashSet.Add(policy.Name) == false)
                    throw new InvalidOperationException($"Policy names must be unique, policy with the name '{policy.Name}' has duplicates.");

                var prev = GetPreviousPolicy(index + 1);
                if (prev.RetentionTime < policy.AggregationTime)
                    throw new InvalidOperationException(
                        $"The policy '{prev.Name}' has a retention time of '{prev.RetentionTime}' " +
                        $"but should be aggregated by policy '{policy.Name}' with the aggregation time frame of {policy.AggregationTime}");

                _policyIndexCache[policy.Name] = index + 1;
            }
        }

        private readonly ConcurrentDictionary<string, int> _policyIndexCache = new ConcurrentDictionary<string, int>(StringComparer.InvariantCultureIgnoreCase);

        internal TimeSeriesPolicy GetPolicyByName(string policy, out int policyIndex)
        {
            if (_policyIndexCache.TryGetValue(policy, out policyIndex))
            {
                if (policyIndex == 0)
                    return RawPolicy;
                return Policies[policyIndex - 1];
            }

            if (policy == RawTimeSeriesPolicy.PolicyString)
            {
                _policyIndexCache[policy] = 0;
                policyIndex = 0;
                return RawPolicy;
            }

            for (var index = 0; index < Policies.Count; index++)
            {
                var p = Policies[index];
                policyIndex = index + 1;
                if (policy.IndexOf(p.Name, StringComparison.InvariantCultureIgnoreCase) == 0)
                {
                    _policyIndexCache[policy] = policyIndex;
                    return p;
                }
            }

            policyIndex = -1;
            return null;
        }

        internal int GetPolicyIndexByTimeSeries(string name)
        {
            var separatorIndex = name.IndexOf(TimeSeriesConfiguration.TimeSeriesRollupSeparator);
            if (separatorIndex <= 0)
                return 0;

            var startIndex = separatorIndex + 1;
            var policyName = name.Substring(startIndex, name.Length - startIndex);

            GetPolicyByName(policyName, out var index);
            return index;
        }

        internal TimeSeriesPolicy GetNextPolicy(int policyIndex)
        {
            if (Policies.Count == 0)
                return null;

            if (policyIndex == Policies.Count)
                return TimeSeriesPolicy.AfterAllPolices;

            return Policies[policyIndex];
        }

        internal TimeSeriesPolicy GetPreviousPolicy(int policyIndex)
        {
            if (policyIndex == 0)
                return TimeSeriesPolicy.BeforeAllPolices;

            if (policyIndex == 1)
                return RawPolicy;

            return Policies[policyIndex - 1];
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Policies)] = new DynamicJsonArray(Policies.Select(p=>p.ToJson())),
                [nameof(RawPolicy)] = RawPolicy.ToJson(),
                [nameof(Disabled)] = Disabled
            };
        }
    }

    public class RawTimeSeriesPolicy : TimeSeriesPolicy
    {
        internal const string PolicyString = "rawpolicy"; // must be lower case

        public static RawTimeSeriesPolicy Default => new RawTimeSeriesPolicy();

        public RawTimeSeriesPolicy()
        {
            Name = PolicyString;
            RetentionTime = TimeSpan.MaxValue;
        }

        public RawTimeSeriesPolicy(TimeSpan retentionTime)
        {
            if (retentionTime <= TimeSpan.Zero)
                throw new ArgumentException("Must be greater than zero", nameof(retentionTime));

            Name = PolicyString;
            RetentionTime = retentionTime;
        }
    }

    public class TimeSeriesPolicy : IDynamicJson, IComparable<TimeSeriesPolicy>
    {
        /// <summary>
        /// Name of the time series policy, must be unique.
        /// </summary>
        public string Name { get; protected set; }

        /// <summary>
        /// How long the data of this policy will be retained
        /// </summary>
        public TimeSpan RetentionTime { get; protected set; }

        /// <summary>
        /// Define the aggregation of this policy
        /// </summary>
        public TimeSpan AggregationTime { get; private set; }


        internal static TimeSeriesPolicy AfterAllPolices = new TimeSeriesPolicy();
        internal static TimeSeriesPolicy BeforeAllPolices = new TimeSeriesPolicy();
        
        protected TimeSeriesPolicy()
        {
        }

        public string GetTimeSeriesName(string rawName)
        {
            return $"{rawName}{TimeSeriesConfiguration.TimeSeriesRollupSeparator}{Name}";
        }

        public TimeSeriesPolicy(string name, TimeSpan aggregationTime) : this(name, aggregationTime, TimeSpan.MaxValue)
        {
        }
        
        public TimeSeriesPolicy(string name, TimeSpan aggregationTime, TimeSpan retentionTime)
        {
            if(string.IsNullOrEmpty(name))
                throw new ArgumentNullException(name);

            if (aggregationTime <= TimeSpan.Zero)
                throw new ArgumentException("Must be greater than zero", nameof(aggregationTime));

            if (retentionTime <= TimeSpan.Zero)
                throw new ArgumentException("Must be greater than zero", nameof(retentionTime));

            RetentionTime = retentionTime;
            AggregationTime = aggregationTime;

            Name = name;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(RetentionTime)] = RetentionTime,
                [nameof(AggregationTime)] = AggregationTime
            };
        }

        protected bool Equals(TimeSeriesPolicy other)
        {
            return RetentionTime == other.RetentionTime &&
                   AggregationTime == other.AggregationTime;
        }

        public int CompareTo(TimeSeriesPolicy other)
        {
            return TimeSeriesDownSamplePolicyComparer.Instance.Compare(this, other);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((TimeSeriesPolicy)obj);
        }

        public override int GetHashCode()
        {
            var hashCode = Hashing.XXHash64.Calculate(Name.ToLowerInvariant(), Encodings.Utf8);
            hashCode = (hashCode * 397) ^ (ulong)RetentionTime.GetHashCode();
            hashCode = (hashCode * 397) ^ (ulong)AggregationTime.GetHashCode();
            return (int)hashCode;
        }
    }

    internal class TimeSeriesDownSamplePolicyComparer : IComparer<TimeSeriesPolicy>
    {
        public static TimeSeriesDownSamplePolicyComparer Instance = new TimeSeriesDownSamplePolicyComparer();

        public int Compare(TimeSeriesPolicy x, TimeSeriesPolicy y)
        {
            if (x == null && y == null)
                return 0;
            if (x == null)
                return 1;
            if (y == null)
                return -1;

            var diff = x.AggregationTime.Ticks - y.AggregationTime.Ticks; // we can't cast to int, since it might overflow

            if (diff > 0)
                return 1;
            if (diff < 0)
                return -1;
            return 0;
        }
    }
}
