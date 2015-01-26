// -----------------------------------------------------------------------
//  <copyright file="CpuStatistics.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Management;
using System.Threading;

using Raven.Abstractions.Logging;
using Raven.Database.Util;

namespace Raven.Database.Config
{
	public static class CpuStatistics
	{
		private const int NotificationThreshold = 80;

		private const int NumberOfItemsInQueue = 5;

		private static readonly int[] LastUsages = new int[NumberOfItemsInQueue];

		private static readonly ConcurrentSet<WeakReference<ICpuUsageHandler>> CpuUsageHandlers = new ConcurrentSet<WeakReference<ICpuUsageHandler>>();

		private static readonly ILog Log = LogManager.GetCurrentClassLogger();

		private static volatile bool domainUnloadOccured;

		private static int nextWriteIndex;

		static CpuStatistics()
		{
			var searcher = new ManagementObjectSearcher("select * from Win32_PerfFormattedData_PerfOS_Processor WHERE Name='_Total'");

			AppDomain.CurrentDomain.DomainUnload += (sender, args) => domainUnloadOccured = true;

			new Thread(() =>
			{
				while (true)
				{
					if (domainUnloadOccured)
						return;

					var totalUsage = searcher
						.Get()
						.Cast<ManagementObject>()
						.Select(x => int.Parse(x["PercentProcessorTime"].ToString()))
						.FirstOrDefault();

					HandleCpuUsage(totalUsage);

					Thread.Sleep(TimeSpan.FromSeconds(1));
				}
			})
			{
				IsBackground = true,
				Name = "CPU usage notification thread"
			}.Start();
		}

		private static void HandleCpuUsage(int usageInPercents)
		{
			var previousWriteIndex = nextWriteIndex;
			LastUsages[previousWriteIndex] = usageInPercents;
			nextWriteIndex = (nextWriteIndex + 1) % NumberOfItemsInQueue;

			if (previousWriteIndex < NumberOfItemsInQueue - 1) // waiting for queue to fill up
				return;

			var average = LastUsages.Average();
			if (average >= NotificationThreshold)
				RunCpuUsageHandlers(handler => handler.HandleHighCpuUsage());
			else
				RunCpuUsageHandlers(handler => handler.HandleLowCpuUsage());

			nextWriteIndex = 0;
		}

		public static void RegisterCpuUsageHandler(ICpuUsageHandler handler)
		{
			CpuUsageHandlers.Add(new WeakReference<ICpuUsageHandler>(handler));
		}

		private static void RunCpuUsageHandlers(Action<ICpuUsageHandler> action)
		{
			var inactiveHandlers = new List<WeakReference<ICpuUsageHandler>>();

			foreach (var highCpuUsageHandler in CpuUsageHandlers)
			{
				ICpuUsageHandler handler;
				if (highCpuUsageHandler.TryGetTarget(out handler))
				{
					try
					{
						action(handler);
					}
					catch (Exception e)
					{
						Log.Error("Failure to process CPU usage notification (cpu usage handler - " + handler + ")", e);
					}
				}
				else
				{
					inactiveHandlers.Add(highCpuUsageHandler);
				}
			}

			inactiveHandlers.ForEach(x => CpuUsageHandlers.TryRemove(x));
		}
	}

	public interface ICpuUsageHandler
	{
		void HandleHighCpuUsage();

		void HandleLowCpuUsage();
	}
}