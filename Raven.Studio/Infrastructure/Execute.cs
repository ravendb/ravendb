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
				return null;
			}
			return Deployment.Current.Dispatcher.InvokeAsync(action)
				.Catch();
		}

		public static Task<TResult> OnTheUI<TResult>(Func<TResult> action)
		{
			if (Deployment.Current.Dispatcher.CheckAccess())
			{
				action();
				return null;
			}
			return Deployment.Current.Dispatcher.InvokeAsync(action)
				.Catch();
		}
	}
}