// -----------------------------------------------------------------------
//  <copyright file="DebugLogger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using Caliburn.Micro;

namespace Raven.Studio.Framework
{
	public class DebugLogger : ILog
	{
		#region Fields

		private readonly Type _type;

		#endregion

		#region Constructors

		public DebugLogger(Type type)
		{
			_type = type;
		}

		#endregion

		#region Helper Methods

		private string CreateLogMessage(string format, params object[] args)
		{
			return string.Format("[{0}] {1}",
			                     DateTime.Now.ToString("o"),
			                     string.Format(format, args));
		}

		#endregion

		#region ILog Members

		public void Error(Exception exception)
		{
			Debug.WriteLine(CreateLogMessage(exception.ToString()), "ERROR");
		}

		public void Info(string format, params object[] args)
		{
			Debug.WriteLine(CreateLogMessage(format, args), "INFO");
		}

		public void Warn(string format, params object[] args)
		{
			Debug.WriteLine(CreateLogMessage(format, args), "WARN");
		}

		#endregion
	}
}