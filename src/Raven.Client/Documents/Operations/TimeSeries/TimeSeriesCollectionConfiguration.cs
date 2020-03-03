using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesCollectionConfiguration : IDynamicJson
    {
        public bool Disabled;

        public List<RollupPolicy> RollupPolicies;

        public TimeSpan? RawDataRetentionTime;

        public void Validate()
        {
            if (RollupPolicies.Count == 0)
                return;

            RollupPolicies.Sort(TimeSeriesDownSamplePolicyComparer.Instance);
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(RollupPolicies)] = new DynamicJsonArray(RollupPolicies.Select(p=>p.ToJson())),
                [nameof(RawDataRetentionTime)] = RawDataRetentionTime,
                [nameof(Disabled)] = Disabled
            };
        }
    }

    public class RollupPolicy : IDynamicJson, IComparable<RollupPolicy>
    {
        /// <summary>
        /// Name of the time series policy, defined by the convention "KeepFor{RetentionTime}AggregatedBy{TimeSpan} (e.g. keep for 12 hours and aggregate by 1 minute = KeepFor12:00:00AggregatedBy00:01:00)"
        /// </summary>
        public string Name; // defined by convention

        /// <summary>
        /// How long the data of this policy will be retained
        /// </summary>
        public TimeSpan RetentionTime;

        /// <summary>
        /// Define the aggregation of this policy
        /// </summary>
        public TimeSpan AggregateBy;

        private RollupPolicy()
        {
            
        }

        public RollupPolicy(TimeSpan retentionTime, TimeSpan aggregateBy)
        {
            RetentionTime = retentionTime;
            AggregateBy = aggregateBy;

            Name = $"KeepFor{RetentionTime}AggregatedBy{aggregateBy}";
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(RetentionTime)] = RetentionTime,
                [nameof(AggregateBy)] = AggregateBy,
            };
        }

        protected bool Equals(RollupPolicy other)
        {
            Debug.Assert(Name == other.Name);
            return RetentionTime == other.RetentionTime &&
                   AggregateBy == other.AggregateBy;
        }

        public int CompareTo(RollupPolicy other)
        {
            return TimeSeriesDownSamplePolicyComparer.Instance.Compare(this, other);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((RollupPolicy)obj);
        }

        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }
    }

    internal class TimeSeriesDownSamplePolicyComparer : IComparer<RollupPolicy>
    {
        public static TimeSeriesDownSamplePolicyComparer Instance = new TimeSeriesDownSamplePolicyComparer();

        public int Compare(RollupPolicy x, RollupPolicy y)
        {
            if (x == null && y == null)
                return 0;
            if (x == null)
                return 1;
            if (y == null)
                return -1;

            var diff = x.AggregateBy.Ticks - y.AggregateBy.Ticks; // we can't cast to int, since it might overflow

            if (diff > 0)
                return 1;
            if (diff < 0)
                return -1;
            return 0;
        }
    }
}
