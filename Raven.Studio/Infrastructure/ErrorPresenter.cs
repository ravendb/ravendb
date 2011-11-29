// -----------------------------------------------------------------------
//  <copyright file="ErrorPresenter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using Raven.Studio.Features.Util;

namespace Raven.Studio.Infrastructure
{
	public static class ErrorPresenter
	{
		private static bool isErrorWindowVisible;

		public static void Show(Uri uri, Exception e)
		{
			var message = string.Format("Could not load page: {0}. {2}Error Message: {1}", uri, e.Message, Environment.NewLine);
			Show(message, e.ToString());
		}

		public static void Show(Exception e)
		{
			Show(e.Message, e.StackTrace);
		}

		public static void Show(Exception e, StackTrace innerStackTrace)
		{
			var details = e +
			              Environment.NewLine + Environment.NewLine +
			              "Inner StackTrace: " + Environment.NewLine +
						  (innerStackTrace == null ? "null" : innerStackTrace.ToString());
			Show(e.Message, details);
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