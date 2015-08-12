// -----------------------------------------------------------------------
//  <copyright file="MonitorOptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Monitor
{
	internal class MonitorOptions
	{
		public MonitorOptions()
		{
			IoOptions = new IoOptions
			{
			    DurationInMinutes = 1
			};
            Action=MonitorActions.DiskIo;
		}

		public MonitorActions Action { get; set; }

		public int ProcessId { get; set; }

		public string ServerUrl { get; set; }

		public IoOptions IoOptions { get; private set; }
	}

	internal class IoOptions
	{
		public int DurationInMinutes { get; set; }
	}
}