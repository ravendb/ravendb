using System;
using Raven.Abstractions.Logging;
using Raven.Database.Util;

namespace Raven.Database.Impl
{
	public class ExceptionAggregator
	{
		private readonly ILog log;
		private readonly string errorMsg;
		readonly ConcurrentSet<Exception> list = new ConcurrentSet<Exception>();

		public ExceptionAggregator(ILog log, string errorMsg)
		{
			this.log = log;
			this.errorMsg = errorMsg;
		}

		public void Execute(Action action)
		{
			try
			{
				action();
			}
			catch (Exception e)
			{
				list.Add(e);
			}
		}

		public void ThrowIfNeeded()
		{
			if (list.Count == 0)
				return;

			var aggregateException = new AggregateException(list);
			log.ErrorException(errorMsg, aggregateException);
			throw aggregateException;
		}
	}
}