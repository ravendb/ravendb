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
        public List<TimeSeriesPolicy> Policies { get; set; }

        /// <summary>
        /// Specify a policy for the original time-series
        /// </summary>
        public RawTimeSeriesPolicy RawPolicy { get; set; }

        protected bool Equals(TimeSeriesCollectionConfiguration other)
        {
            try
            {
                other.ValidateAndInitialize();
            }
            catch
            {
                return false;
            }

            if (Disabled != other.Disabled)
                return false;

            if (Equals(RawPolicy, other.RawPolicy) == false)
                return false;

            if (Policies.Count != other.Policies.Count)
                return false;

            return Policies.SequenceEqual(other.Policies);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (obj.GetType() != GetType())
                return false;
            return Equals((TimeSeriesCollectionConfiguration)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Disabled.GetHashCode();
                if (RawPolicy != null)
                    hashCode = (hashCode * 397) ^ RawPolicy.GetHashCode();

                if (Policies != null)
                {
                    foreach (var policy in Policies)
                    {
                        hashCode = (hashCode * 397) ^ policy.GetHashCode();
                    }
                }

                return hashCode;
            }
        }

        internal void ValidateAndInitialize()
        {
            RawPolicy ??= RawTimeSeriesPolicy.Default;

            if (RawPolicy.RetentionTime <= TimeValue.Zero)
                throw new InvalidOperationException("Retention time of the RawPolicy must be greater than zero");

            if (Policies == null)
                return;

            if (Policies.Count == 0)
                return;

            Policies.Sort(TimeSeriesDownSamplePolicyComparer.Instance);

            var hashSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (var index = 0; index < Policies.Count; index++)
            {
                var policy = Policies[index];
                if (hashSet.Add(policy.Name) == false)
                    throw new InvalidOperationException($"Policy names must be unique, policy with the name '{policy.Name}' has duplicates.");

                if (policy.AggregationTime <= TimeValue.Zero)
                    throw new InvalidOperationException($"Aggregation time of '{policy.Name}' must be greater than zero");

                if (policy.RetentionTime <= TimeValue.Zero)
                    throw new InvalidOperationException($"Retention time of '{policy.Name}' must be greater than zero");

                var prev = GetPreviousPolicy(index + 1);
                if (prev.AggregationTime == policy.AggregationTime)
                    throw new InvalidOperationException(
                        $"The policy '{prev.Name}' has the same aggregation time as the policy '{policy.Name}'");

                if (prev.RetentionTime < policy.AggregationTime)
                    throw new InvalidOperationException(
                        $"The policy '{prev.Name}' has a retention time of '{prev.RetentionTime}' " +
                        $"but should be aggregated by policy '{policy.Name}' with the aggregation time frame of {policy.AggregationTime}");

                if (index > 0 && // first policy always legit, since the source is the raw data
                    prev.AggregationTime.IsMultiple(policy.AggregationTime) == false)
                    throw new InvalidOperationException($"The aggregation time of the policy '{policy.Name}' ({policy.AggregationTime}) must be divided by the aggregation time of '{prev.Name}' ({prev.AggregationTime}) without a remainder.");

                _policyIndexCache[policy.Name] = index + 1;
            }
        }

        private readonly ConcurrentDictionary<string, int> _policyIndexCache = new ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase);

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
                if (policy.IndexOf(p.Name, StringComparison.OrdinalIgnoreCase) == 0)
                {
                    _policyIndexCache[policy] = policyIndex;
                    return p;
                }
            }

            policyIndex = -1;
            return null;
        }

        internal TimeSeriesPolicy GetNextPolicy(TimeSeriesPolicy policy)
        {
            GetPolicyByName(policy.Name, out var index);
            return GetNextPolicy(index);
        }

        internal TimeValue MaxRetention
        {
            get
            {
                if (_maxRetention.HasValue)
                    return _maxRetention.Value;

                if (Policies == null || Policies.Count == 0)
                    return TimeValue.MaxValue;

                _maxRetention = Policies.Last().RetentionTime;
                return _maxRetention.Value;
            }
        }

        private TimeValue? _maxRetention;

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

        internal TimeSeriesPolicy GetPolicy(int policyIndex)
        {
            if (policyIndex == 0)
                return RawPolicy;

            return Policies[policyIndex - 1];
        }

        internal TimeSeriesPolicy GetNextPolicy(int policyIndex)
        {
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

            return Policies[policyIndex - 2];
        }

        public DynamicJsonValue ToJson()
        {
            var config = new DynamicJsonValue
            {
                [nameof(RawPolicy)] = RawPolicy?.ToJson(),
                [nameof(Disabled)] = Disabled
            };

            if (Policies != null)
                config[nameof(Policies)] = new DynamicJsonArray(Policies.Select(p => p.ToJson()));

            return config;
        }
    }

    public class RawTimeSeriesPolicy : TimeSeriesPolicy
    {
        internal const string PolicyString = "rawpolicy"; // must be lower case

        public static RawTimeSeriesPolicy Default => new RawTimeSeriesPolicy();

        public RawTimeSeriesPolicy()
        {
            Name = PolicyString;
            RetentionTime = TimeValue.MaxValue;
        }

        public RawTimeSeriesPolicy(TimeValue retentionTime)
        {
            if (retentionTime <= TimeValue.Zero)
                throw new ArgumentException("Must be greater than zero", nameof(retentionTime));

            Name = PolicyString;
            RetentionTime = retentionTime;
        }

        public static bool IsRaw(TimeSeriesPolicy policy) => policy.Name == PolicyString;
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
        public TimeValue RetentionTime { get; protected set; }

        /// <summary>
        /// Define the aggregation of this policy
        /// </summary>
        public TimeValue AggregationTime { get; private set; }

        internal static TimeSeriesPolicy AfterAllPolices = new TimeSeriesPolicy();
        internal static TimeSeriesPolicy BeforeAllPolices = new TimeSeriesPolicy();

        protected TimeSeriesPolicy()
        {
        }

        public string GetTimeSeriesName(string rawName)
        {
            if (Name == RawTimeSeriesPolicy.PolicyString)
                return rawName;

            return $"{rawName}{TimeSeriesConfiguration.TimeSeriesRollupSeparator}{Name}";
        }

        public TimeSeriesPolicy(string name, TimeValue aggregationTime) : this(name, aggregationTime, TimeValue.MaxValue)
        {
        }

        public TimeSeriesPolicy(string name, TimeValue aggregationTime, TimeValue retentionTime)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(name);

            if (aggregationTime <= TimeValue.Zero)
                throw new ArgumentException("Must be greater than zero", nameof(aggregationTime));

            if (retentionTime <= TimeValue.Zero)
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
                   AggregationTime == other.AggregationTime &&
                   string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase);
        }

        public int CompareTo(TimeSeriesPolicy other)
        {
            return TimeSeriesDownSamplePolicyComparer.Instance.Compare(this, other);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((TimeSeriesPolicy)obj);
        }

        public override int GetHashCode()
        {
            var hashCode = StringComparer.OrdinalIgnoreCase.GetHashCode(Name);
            hashCode = (hashCode * 397) ^ RetentionTime.GetHashCode();
            hashCode = (hashCode * 397) ^ AggregationTime.GetHashCode();
            return hashCode;
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

            return x.AggregationTime.Compare(y.AggregationTime);
        }
    }
}
