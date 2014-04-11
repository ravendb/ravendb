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
}