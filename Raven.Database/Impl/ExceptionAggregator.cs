using System;
using System.Collections.Generic;
using NLog;
using Raven.Database.Util;

namespace Raven.Database.Impl
{
	public class ExceptionAggregator
	{
		private readonly Logger log;
		private readonly string errorMsg;
		readonly ConcurrentSet<Exception> list = new ConcurrentSet<Exception>();

		public ExceptionAggregator(Logger log, string errorMsg)
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