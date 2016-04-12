using System;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplicationPerformanceStats
    {
        public int BatchSize { get; set; }
        public TimeSpan Duration { get; set; }
        public DateTime Started { get; set; }

        public double DurationMilliseconds => Math.Round(Duration.TotalMilliseconds, 2);

        public override string ToString()
        {
            return $"BatchSize: {BatchSize}, Started: {Started}, Duration: {Duration}";
        }

        protected bool Equals(SqlReplicationPerformanceStats other)
        {
            return BatchSize == other.BatchSize && Duration.Equals(other.Duration) && Started.Equals(other.Started);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((SqlReplicationPerformanceStats)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = BatchSize;
                hashCode = (hashCode * 397) ^ Duration.GetHashCode();
                hashCode = (hashCode * 397) ^ Started.GetHashCode();
                return hashCode;
            }
        }
    }
}