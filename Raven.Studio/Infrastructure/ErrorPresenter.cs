// -----------------------------------------------------------------------
//  <copyright file="ErrorPresenter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.Studio.Features.Errors;

namespace Raven.Studio.Infrastructure
{
	public static class ErrorPresenter
	{
		private static bool isErrorWindowVisible;

		public static void Show(Uri uri, Exception e)
		{
			var message = string.Format("Could not load page: {0}. {2}Error Message: {1}", uri, e.Message, Environment.NewLine);
			Show(message, e.StackTrace);
		}

		public static void Show(Exception e)
		{
			Show(e.Message, e.StackTrace);
		}

		public static void Show(string message, string details)
		{
			if (isErrorWindowVisible)
				return;

			isErrorWindowVisible = true;
			var window = new ErrorWindow(message, details);
			window.Closed += (sender, args) => isErrorWindowVisible = false;
			window.Show();
		}
	}
}