// -----------------------------------------------------------------------
//  <copyright file="LoggingErrorHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Nancy;
using Nancy.ErrorHandling;
using Raven.Abstractions.Logging;

namespace Raven.ClusterManager
{
	public class LoggingErrorHandler : IStatusCodeHandler
	{
		private readonly static ILog Log = LogManager.GetCurrentClassLogger();

		public bool HandlesStatusCode(HttpStatusCode statusCode, NancyContext context)
		{
			if (statusCode == HttpStatusCode.OK)
				return false;

			return true;
		}

		public void Handle(HttpStatusCode statusCode, NancyContext context)
		{
			Log.Info("status code: {0}", statusCode);
			Log.Info("Url: {0}", context.Request.Url);

			object errorObject;
			if (context.Items.TryGetValue(NancyEngine.ERROR_EXCEPTION, out errorObject))
			{
				Log.ErrorException("Unhandled error", errorObject as Exception);
			}
		}
	}
}