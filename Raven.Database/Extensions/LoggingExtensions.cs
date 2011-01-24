//-----------------------------------------------------------------------
// <copyright file="LoggingExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Globalization;
using log4net;
using log4net.Core;
using log4net.Util;

namespace Raven.Database.Extensions
{
	public static class LoggingExtensions
	{
		public static void WarnFormat(this ILog self, Exception e, string format, params object[] parameters)
		{
			if (self.IsWarnEnabled == false)
				return;
			self.Logger.Log(typeof (LogImpl), Level.Warn,
			                new SystemStringFormat(CultureInfo.InvariantCulture, format, parameters), e);
		}

		public static void ErrorFormat(this ILog self, Exception e, string format, params object[] parameters)
		{
			if (self.IsErrorEnabled == false)
				return;
			self.Logger.Log(typeof (LogImpl), Level.Error,
			                new SystemStringFormat(CultureInfo.InvariantCulture, format, parameters), e);
		}
	}
}
