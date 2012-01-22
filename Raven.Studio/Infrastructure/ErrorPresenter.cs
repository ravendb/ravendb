// -----------------------------------------------------------------------
//  <copyright file="ErrorPresenter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.IO;
using Raven.Studio.Features.Util;

namespace Raven.Studio.Infrastructure
{
	public static class ErrorPresenter
	{
		private static bool isErrorWindowVisible;

		public static void Show(Exception e, StackTrace innerStackTrace = null, string customMessage = null)
		{
			var writer = new StringWriter();

			if (customMessage != null)
			{
				writer.WriteLine(customMessage);
				writer.WriteLine();
				writer.WriteLine();
			}

			writer.WriteLine(e.Message);
			writer.WriteLine();
			writer.WriteLine(e.ToString());
			writer.WriteLine();

			if (innerStackTrace != null)
			{
				writer.WriteLine();
				writer.WriteLine("Inner StackTrace: ");
				writer.WriteLine(innerStackTrace.ToString());
				writer.WriteLine();
			}

			writer.WriteLine("-- Additional Information --");
			writer.Write("Uri: ");
			writer.WriteLine(UrlUtil.Url);
			writer.Write("Server Uri: ");
			writer.WriteLine(GetServerUri(e) ?? "null");

			Show(writer.ToString());
		}

		public static void Show(string text)
		{
			if (isErrorWindowVisible)
				return;

			isErrorWindowVisible = true;

			var window = new ErrorWindow(text);
			window.Closed += (sender, args) => isErrorWindowVisible = false;
			window.Show();
		}

		private static string GetServerUri(Exception e)
		{
			if (e.Data.Contains("Url"))
				return e.Data["Url"] as string;
			
			if (e.InnerException != null)
				return GetServerUri(e.InnerException);
			
			return null;
		}
	}
}