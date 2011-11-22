using System;
using System.Security;
using Raven.Studio.Messages;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
	public static class ErrorHandler
	{
		public static bool Handle(Exception ex)
		{
			if (ex is SecurityException)
			{
				Error("Cannot connect to the server");
				return true;
			}
			return false;
		}

		private static void Error(string message)
		{
			ApplicationModel.Current.AddNotification(new Notification(message, NotificationLevel.Error));
		}
	}
}