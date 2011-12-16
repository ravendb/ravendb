using System;
using System.Collections.Generic;

namespace Raven.Database.Impl
{
	public class ExceptionAggregator
	{
		List<Exception> list = new List<Exception>();

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

			throw new AggregateException(list);
		}
	}
}