// -----------------------------------------------------------------------
//  <copyright file="AsyncHelpers.cs" company="Hibernating Rhinos LTD">
//      Copyright (coffee) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util.Schedulers;

namespace Raven.Abstractions.Util
{
	public static class AsyncHelpers
	{
		private static readonly TaskFactory Factory = new TaskFactory(new CurrentThreadTaskScheduler());

		public static T RunSync<T>(Func<Task<T>> work)
		{
			var result = default(T);
			try
			{
				result = Factory
					.StartNew(work)
					.Unwrap()
					.ConfigureAwait(false)
					.GetAwaiter()
					.GetResult();
			}
			catch (AggregateException ex)
			{
				var exception = ex.ExtractSingleInnerException();
				ExceptionDispatchInfo.Capture(exception).Throw();
			}

			return result;
		}

		public static void RunSync(Func<Task> work)
		{
			try
			{
				Factory
					.StartNew(work)
					.Unwrap()
					.ConfigureAwait(false)
					.GetAwaiter()
					.GetResult();
			}
			catch (AggregateException ex)
			{
				var exception = ex.ExtractSingleInnerException();
				ExceptionDispatchInfo.Capture(exception).Throw();
			}
		}
	}
}