using System;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;

namespace Raven.Database.Bundles.SqlReplication
{
	public class SqlReplicationStatistics
	{
		private readonly string name;

		public SqlReplicationStatistics(string name)
		{
			this.name = name;
		}

		public DateTime LastErrorTime { get; private set; }
		private int ScriptErrorCount { get; set; }
		private int ScriptSuccessCount { get; set; }
		private int WriteErrorCount { get; set; }
		private int SuccessCount { get; set; }

		public Alert LastAlert { get; set; }

		public void Success(int countOfItems)
		{
			LastErrorTime = DateTime.MinValue;
			SuccessCount += countOfItems;
		}

		public void RecordWriteError(Exception e, DocumentDatabase database, int count = 1, DateTime? newErrorTime = null)
		{
			WriteErrorCount += count;


            LastAlert = new Alert
			{
				AlertLevel = AlertLevel.Error,
				CreatedAt = SystemTime.UtcNow,
				Message = "Last SQL eplication operation for " + name + " was failed",
				Title = "SQL replication error",
				Exception = e.ToString(),
				UniqueKey = "Sql Replication Error: " + name
			};

			if (WriteErrorCount < 100)
				return;

			if (WriteErrorCount <= SuccessCount)
				return;
			if (newErrorTime != null)
			{
				LastErrorTime = newErrorTime.Value;
				return;
			}

			database.AddAlert(LastAlert = new Alert
			{
				AlertLevel = AlertLevel.Error,
				CreatedAt = SystemTime.UtcNow,
				Message = "Could not tolerate write error ratio and stopped current replication cycle for " + name + Environment.NewLine + this,
				Title = "Sql Replication write error hit ratio too high",
				Exception = e.ToString(),
				UniqueKey = "Sql Replication Write Error Ratio: " + name
			});

			throw new InvalidOperationException("Could not tolerate write error ratio and stopped current replication cycle for " + name + Environment.NewLine + this, e);
		}

		public override string ToString()
		{
			return string.Format("LastErrorTime: {0}, ScriptErrorCount: {1}, WriteErrorCount: {2}, SuccessCount: {3}",
			                     LastErrorTime, ScriptErrorCount, WriteErrorCount, SuccessCount);
		}

		public void CompleteSuccess(int countOfItems)
		{
			Success(countOfItems);
			WriteErrorCount /= 2;
			ScriptErrorCount /= 2;
		}

		public void MarkScriptAsInvalid(DocumentDatabase database, string script)
		{
			ScriptErrorCount = int.MaxValue;
			LastErrorTime = DateTime.MaxValue;
			database.AddAlert(LastAlert = new Alert
			{
				AlertLevel = AlertLevel.Error,
				CreatedAt = SystemTime.UtcNow,
				Message = string.Format("Could not parse script for {0} " + Environment.NewLine + "Script: {1}", name, script),
				Title = "Could not parse Script",
				UniqueKey = "Script Parse Error: " + name
			});
		}

		public void RecordScriptError(DocumentDatabase database)
		{
			ScriptErrorCount++;

			if (ScriptErrorCount < 100)
				return;

			if (ScriptErrorCount <= ScriptSuccessCount)
				return;

			database.AddAlert(LastAlert = new Alert
			{
				AlertLevel = AlertLevel.Error,
				CreatedAt = SystemTime.UtcNow,
				Message = "Could not tolerate script error ratio and stopped current replication cycle for " + name + Environment.NewLine + this,
				Title = "Sql Replication script error hit ratio too high",
				UniqueKey = "Sql Replication Script Error Ratio: " + name
			});

			throw new InvalidOperationException("Could not tolerate script error ratio and stopped current replication cycle for " + name + Environment.NewLine + this);
		}

		public void ScriptSuccess()
		{
			ScriptSuccessCount++;
		}
	}

    public class SqlReplicationPerformanceStats
    {
        public int BatchSize { get; set; }
        public TimeSpan Duration { get; set; }
        public DateTime Started { get; set; }
        public double DurationMilliseconds { get { return Math.Round(Duration.TotalMilliseconds, 2); } }

        public override string ToString()
        {
            return string.Format("BatchSize: {0}, Started: {1}, Duration: {2}", BatchSize, Started, Duration);
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
            return Equals((SqlReplicationPerformanceStats) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = BatchSize;
                hashCode = (hashCode*397) ^ Duration.GetHashCode();
                hashCode = (hashCode*397) ^ Started.GetHashCode();
                return hashCode;
            }
        }
    }
}