// -----------------------------------------------------------------------
//  <copyright file="LogItem.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Raven.Abstractions.Data
{
	public class LogItem
	{
		public string Name { get; set; }
		public string Message { get; set; }
		public string LoggerName { get; set; }
		public string Exception { get; set; }
		public DateTime Timestamp { get; set; } 
	}
}