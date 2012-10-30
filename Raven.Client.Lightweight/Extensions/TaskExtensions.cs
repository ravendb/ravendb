using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;

namespace Raven.Client.Extensions
{
	public static class Time
	{
		public static Task Delay(TimeSpan timeOut)
		{
			var tcs = new TaskCompletionSource<object>();

			var timer = new Timer(tcs.SetResult,
			                      null,
			                      timeOut,
			                      TimeSpan.FromMilliseconds(-1));

			return tcs.Task.ContinueWith(_ => timer.Dispose(), TaskContinuationOptions.ExecuteSynchronously);
		}
	}

	public static class TaskExtensions
	{
		public static void AssertNotFailed(this Task task)
		{
			if (task.IsFaulted)
				task.Wait(); // would throw
		}

		public static Task<T> ConvertSecurityExceptionToServerNotFound<T>(this Task<T> parent)
		{
			return parent.ContinueWith(task =>
										{
											if (task.IsFaulted)
											{
												var exception = task.Exception.ExtractSingleInnerException();
												if (exception is SecurityException)
													throw new WebException("Could not contact server.\r\nGot security error because RavenDB wasn't able to contact the database to get ClientAccessPolicy.xml permission.", exception);
											}
											return task;
										}).Unwrap();
		}


		public static Task<T> AddUrlIfFaulting<T>(this Task<T> parent, Uri uri)
		{
			return parent.ContinueWith(task =>
										{
											if (task.IsFaulted)
											{
												var e = task.Exception.ExtractSingleInnerException();
												if (e != null)
													e.Data["Url"] = uri;
											}

											return task;
										})
				.Unwrap();
		}

		public static Task<object> WithNullResult(this Task task)
		{
			return task.WithResult((object)null);
		}

		public static Task<T> WithResult<T>(this Task task, T result)
		{
			return task.WithResult(() => result);
		}

		public static Task<T> WithResult<T>(this Task task, Func<T> result)
		{
			return task.ContinueWith(t =>
			{
				t.AssertNotFailed();
				return result();
			});
		}

		public static Task<T> WithResult<T>(this Task task, Task<T> result)
		{
			return task.WithResult<Task<T>>(result).Unwrap();
		}

		public static Task<T> ContinueWithTask<T>(this Task task, Func<Task<T>> result)
		{
			return task.WithResult<Task<T>>(result).Unwrap();
		}

		public static Task ContinueWithTask(this Task task, Func<Task> result)
		{
			return task.WithResult<Task>(result).Unwrap();
		}

		public static Task<T[]> StartSequentially<T>(this IEnumerable<Func<Task<T>>> tasks)
		{
			Func<IEnumerable<Func<Task<T>>>, Task<IEnumerable<T>>> executor = null;
			executor = remainingTasks =>
				{
					if (!remainingTasks.Any())
					{
						return CompletedTask.With(Enumerable.Empty<T>());
					}

					var first = remainingTasks.First();
					var rest = remainingTasks.Skip(1);

					return first().ContinueWith(task =>
					{
						var firstResult = task.Result;
						return executor(rest).ContinueWith(restTask =>
						{
							return new[] { firstResult }.Concat(restTask.Result);
						});
					}).Unwrap();
				};

			return executor(tasks.ToList()).ContinueWith(task => task.Result.ToArray());
		}

		public static Task StartSequentially(this IEnumerable<Func<Task>> tasks)
		{
			return tasks.Select(t => new Func<Task<object>>(() => t().WithNullResult())).StartSequentially();
		}

		public static Task<T[]> StartInParallel<T>(this IEnumerable<Func<Task<T>>> tasks)
		{
			var started = tasks.Select(x => x()).ToArray();
			return started.AggregateAsync();
		}

		public static Task StartInParallel(this IEnumerable<Func<Task>> tasks)
		{
			return tasks.Select(t => new Func<Task<object>>(() => t().WithNullResult())).StartInParallel();
		}

		public static Task<T[]> AggregateAsync<T>(this IEnumerable<Task<T>> tasks)
		{
			return AggregateAsync(tasks, Enumerable.ToArray);
		}

		public static Task<TResult> AggregateAsync<T, TResult>(this IEnumerable<Task<T>> tasks, Func<IEnumerable<T>, TResult> aggregation)
		{
			return Task.Factory.ContinueWhenAll(tasks.ToArray(), results =>
			{
				// The cast in the next line is required in Silverlight, because covariance isn't supported there
				var exceptions = results.Where(t => t.IsFaulted).Select(t => (Exception)t.Exception);
				if (exceptions.Any())
					throw new AggregateException(exceptions);

				return aggregation(results.Select(t => t.Result));
			});
		}
	}
}