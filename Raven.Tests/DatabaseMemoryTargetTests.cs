using System;
using System.Linq;
using Raven.Abstractions.Logging;
using Raven.Database.Server;
using Raven.Database.Util;
using Xunit;

namespace Raven.Tests
{
	public class DatabaseMemoryTargetTests : IDisposable
	{
		private const string DatabaseName = "TestDB";
		private readonly DatabaseMemoryTarget sut;

		public DatabaseMemoryTargetTests()
		{
			LogContext.DatabaseName.Value = DatabaseName;
			sut = new DatabaseMemoryTarget();
		}

		public void Dispose()
		{
			LogContext.DatabaseName.Value = null;
		}

		[Fact]
		public void When_logger_name_starts_with_Raven_then_should_log_message()
		{
			var logEventInfo = new LogEventInfo {LoggerName = "Raven.x"};
			sut.Write(logEventInfo);
			Assert.Equal(logEventInfo, sut[DatabaseName].GeneralLog.Single());
		}

		[Fact]
		public void When_logger_name_does_not_start_with_Raven_then_should_not_log_message()
		{
			sut.Write(new LogEventInfo {LoggerName = "Raven.x"});
			sut.Write(new LogEventInfo {LoggerName = "Rave.x"});
			Assert.Equal(1, sut[DatabaseName].GeneralLog.Count());
		}

		[Fact]
		public void Log_events_should_go_to_correct_log()
		{
			sut.Write(new LogEventInfo { LoggerName = "Raven.x" , Level = LogLevel.Info });
			sut.Write(new LogEventInfo { LoggerName = "Raven.x" , Level = LogLevel.Warn });
			Assert.Equal(2, sut[DatabaseName].GeneralLog.Count());
			Assert.Equal(1, sut[DatabaseName].WarnLog.Count());
		}

		[Fact]
		public void Can_clear_logs_for_a_specific_database()
		{
			sut.Clear(DatabaseName);
			Assert.Equal(0, sut.DatabaseTargetCount);
			Assert.Empty(sut[DatabaseName].GeneralLog);
		}

		[Fact]
		public void Can_clear_all_logs()
		{
			sut.ClearAll();
			Assert.Equal(0, sut.DatabaseTargetCount);
		}

		[Fact]
		public void When_context_database_name_is_null_The_should_not_record_log()
		{
			LogContext.DatabaseName.Value = null;
			sut.Write(new LogEventInfo { LoggerName = "Raven.x", Level = LogLevel.Info });
			Assert.Equal(0, sut.DatabaseTargetCount);
		}

		[Fact]
		public void When_numebr_of_log_events_written_exceed_limit_Then_log_count_should_equal_limit()
		{
			for (int i = 0; i < DatabaseMemoryTarget.BoundedMemoryTarget.Limit + 1; i++)
			{
				var logEventInfo = new LogEventInfo { LoggerName = "Raven.x" };
				sut.Write(logEventInfo);
			}

			Assert.Equal(DatabaseMemoryTarget.BoundedMemoryTarget.Limit, sut[DatabaseName].GeneralLog.Count());
		}
	}
}