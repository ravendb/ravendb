// -----------------------------------------------------------------------
//  <copyright file="AsyncHelpers.cs" company="Hibernating Rhinos LTD">
//      Copyright (coffee) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Util
{
	public static class AsyncHelpers
	{
		public static T RunSync<T>(Func<Task<T>> work)
		{
			var result = default(T);
			try
			{
				var taskAwaiter = Task.Run(work).ConfigureAwait(false)
									    .GetAwaiter();				

				result = taskAwaiter.GetResult();
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
				Task.Run(work).ConfigureAwait(false)
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