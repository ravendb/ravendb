using System;
using Raven.Abstractions.Logging;
using Xunit;

namespace Raven.Tests.Abstractions.Logging
{
	public class LoggerExecutionWrapperTests
	{
		private readonly LoggerExecutionWrapper sut;
		private readonly FakeLogger fakeLogger;

		public LoggerExecutionWrapperTests()
		{
			fakeLogger = new FakeLogger();
			sut = new LoggerExecutionWrapper(fakeLogger, "name", new Target[0]);
		}

		[Fact]
		public void When_logging_and_message_factory_throws_Then_should_log_exception()
		{
			var loggingException = new Exception("Message");
			sut.Log(LogLevel.Info, () => { throw loggingException; });
			Assert.Same(loggingException, fakeLogger.Exception);
			Assert.Equal(LoggerExecutionWrapper.FailedToGenerateLogMessage, fakeLogger.Message);
		}

		[Fact]
		public void When_logging_with_exception_and_message_factory_throws_Then_should_log_exception()
		{
			var appException = new Exception("Message");
			var loggingException = new Exception("Message");
			sut.Log(LogLevel.Info, () => { throw loggingException; }, appException);
			Assert.Same(loggingException, fakeLogger.Exception);
			Assert.Equal(LoggerExecutionWrapper.FailedToGenerateLogMessage, fakeLogger.Message);
		}

		public class FakeLogger : ILog
		{
			private LogLevel logLevel;

			public LogLevel LogLevel
			{
				get { return logLevel; }
			}

			public string Message
			{
				get { return message; }
			}

			public Exception Exception
			{
				get { return exception; }
			}

			private string message;
			private Exception exception;

			public bool IsDebugEnabled { get; set; }

			public bool IsWarnEnabled { get; set; }

			public void Log(LogLevel logLevel, Func<string> messageFunc)
			{
				messageFunc();
			}

			public void Log<TException>(LogLevel logLevel, Func<string> messageFunc, TException exception) where TException : Exception
			{
				string message = messageFunc();
				if (message != null)
				{
					this.logLevel = logLevel;
					this.message = messageFunc() ?? this.message;
					this.exception = exception;
				}
			}
		}
	}
}