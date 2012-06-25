namespace Raven.Tests.Abstractions.Logging
{
	using System;
	using Raven.Abstractions.Logging;
	using Xunit;

	public class LoggerExecutionWrapperTests
	{
		private readonly LoggerExecutionWrapper _sut;
		private readonly FakeLogger _fakeLogger;

		public LoggerExecutionWrapperTests()
		{
			_fakeLogger = new FakeLogger();
			_sut = new LoggerExecutionWrapper(_fakeLogger);
		}

		[Fact]
		public void When_logging_and_message_factory_throws_Then_should_log_exception()
		{
			var loggingException = new Exception("Message");
			_sut.Log(LogLevel.Info, () => { throw loggingException; });
			Assert.Same(loggingException, _fakeLogger.Exception);
			Assert.Equal(LoggerExecutionWrapper.FailedToGenerateLogMessage, _fakeLogger.Message);
		}

		[Fact]
		public void When_logging_with_exception_and_message_factory_throws_Then_should_log_exception()
		{
			var appException = new Exception("Message");
			var loggingException = new Exception("Message");
			_sut.Log(LogLevel.Info, () => { throw loggingException; }, appException);
			Assert.Same(loggingException, _fakeLogger.Exception);
			Assert.Equal(LoggerExecutionWrapper.FailedToGenerateLogMessage, _fakeLogger.Message);
		}

		public class FakeLogger : ILog
		{
			private LogLevel _logLevel;

			public LogLevel LogLevel
			{
				get { return _logLevel; }
			}

			public string Message
			{
				get { return _message; }
			}

			public Exception Exception
			{
				get { return _exception; }
			}

			private string _message;
			private Exception _exception;

			public void Log(LogLevel logLevel, Func<string> messageFunc)
			{
				messageFunc();
			}

			public void Log<TException>(LogLevel logLevel, Func<string> messageFunc, TException exception) where TException : Exception
			{
				string message = messageFunc();
				if (message != null)
				{
					_logLevel = logLevel;
					_message = messageFunc() ?? _message;
					_exception = exception;
				}
			}
		}
	}
}