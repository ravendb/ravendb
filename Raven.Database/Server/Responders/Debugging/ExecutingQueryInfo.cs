using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Raven.Abstractions.Data;

namespace Raven.Database.Server.Responders.Debugging
{
    public class ExecutingQueryInfo : IEquatable<ExecutingQueryInfo>
    {
        public DateTime StartTime { get; private set; }

        public IndexQuery QueryInfo { get; private set; }

        public TimeSpan Duration
        {
            get
            {
                return stopwatch.Elapsed;
            }
        }

        private readonly Stopwatch stopwatch;

        public ExecutingQueryInfo(DateTime startTime, IndexQuery queryInfo)
        {
            StartTime = startTime;
            QueryInfo = queryInfo;
            stopwatch = Stopwatch.StartNew();
        }


        public bool Equals(ExecutingQueryInfo other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return StartTime.Equals(other.StartTime) && Equals(QueryInfo, other.QueryInfo);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((ExecutingQueryInfo) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (StartTime.GetHashCode()*397) ^ (QueryInfo != null ? QueryInfo.GetHashCode() : 0);
            }
        }

        public static bool operator ==(ExecutingQueryInfo left, ExecutingQueryInfo right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(ExecutingQueryInfo left, ExecutingQueryInfo right)
        {
            return !Equals(left, right);
        }
    }
}
