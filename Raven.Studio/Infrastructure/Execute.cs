// -----------------------------------------------------------------------
//  <copyright file="Execute.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using System.Windows;

namespace Raven.Studio.Infrastructure
{
	public static class Execute
	{
		public static Task OnTheUI(Action action)
		{
			if (Deployment.Current.Dispatcher.CheckAccess())
			{
				action();
				return EmptyResult<object>();
			}
			return Deployment.Current.Dispatcher.InvokeAsync(action)
				.Catch();
		}

		public static Task<T> EmptyResult<T>()
		{
			var tcs = new TaskCompletionSource<T>();
			tcs.SetResult(default(T));
			return tcs.Task;
		}

		public static Task OnTheUI<TResult>(Func<TResult> action)
		{
			if (Deployment.Current.Dispatcher.CheckAccess())
			{
				action();
				return EmptyResult<TResult>();
			}
			return Deployment.Current.Dispatcher.InvokeAsync(action)
				.Catch();
		}
	}
}