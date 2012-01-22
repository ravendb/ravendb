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

			writer.Write("Message: ");
			writer.WriteLine(e.Message);
			if (string.IsNullOrWhiteSpace(UrlUtil.Url) == false)
			{
				writer.Write("Uri: ");
				writer.WriteLine(UrlUtil.Url);
			}
			writer.Write("Server Uri: ");
			writer.WriteLine(GetServerUri(e));

			writer.WriteLine();
			writer.WriteLine("-- Error Information --");
			writer.WriteLine(e.ToString());
			writer.WriteLine();

			if (innerStackTrace != null)
			{
				writer.WriteLine("Inner StackTrace: ");
				writer.WriteLine(innerStackTrace.ToString());
			}
		
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
			{
				var serverUri = (Uri) e.Data["Url"];
				if(serverUri != null)
					return serverUri.ToString();
				return "null";
			}

			if (e.InnerException != null)
				return GetServerUri(e.InnerException);

			return "unknown";
		}
	}
}