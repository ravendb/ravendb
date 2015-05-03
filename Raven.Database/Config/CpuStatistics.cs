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

using ConfigurationManager = System.Configuration.ConfigurationManager;

namespace Raven.Database.Config
{
	public static class CpuStatistics
	{
		private const int HighNotificationThreshold = 80;
        private const int LowNotificationThreshold = 60;

		private const int NumberOfItemsInQueue = 5;

		private static readonly int[] LastUsages = new int[NumberOfItemsInQueue];

		private static readonly ConcurrentSet<WeakReference<ICpuUsageHandler>> CpuUsageHandlers = new ConcurrentSet<WeakReference<ICpuUsageHandler>>();

		private static readonly ILog Log = LogManager.GetCurrentClassLogger();

	    private static bool errorGettingCpuStats;
		private static int nextWriteIndex;
        private static readonly ManualResetEventSlim _domainUnload = new ManualResetEventSlim();
	    private static bool dynamicLoadBalancding;

		public static double Average { get; private set; }

	    static CpuStatistics()
		{
	        if (bool.TryParse(ConfigurationManager.AppSettings["Raven/DynamicLoadBalancing"], out dynamicLoadBalancding) && 
                dynamicLoadBalancding == false)
		        return; // disabled, so we avoid it
	        dynamicLoadBalancding = true;

		    AppDomain.CurrentDomain.DomainUnload += (sender, args) => _domainUnload.Set();

			new Thread(() =>
			{
			    try
			    {
                    ManagementObjectSearcher searcher;
                    try
			        {
                        searcher = new ManagementObjectSearcher("select * from Win32_PerfFormattedData_PerfOS_Processor WHERE Name='_Total'");
			        }
			        catch (Exception e)
			        {
			            Log.WarnException("Could not get the CPU statistics, automatic CPU thorttling disabled!", e);
			            return;
			        }
			        while (true)
			        {
			            if (_domainUnload.Wait(1000))
			                return;

                        int totalUsage;
                        try
			            {
			                totalUsage = searcher
			                    .Get()
			                    .Cast<ManagementObject>()
			                    .Select(x => int.Parse(x["PercentProcessorTime"].ToString()))
			                    .FirstOrDefault();
			                errorGettingCpuStats = false;
			            }
			            catch (Exception e)
			            {
			                if (errorGettingCpuStats)
			                {
                                Log.WarnException("Repeatedly cannot get CPU usage from system, assuming temporary issue, will try again in a few minutes", 
                                    e);
			                    if (_domainUnload.Wait(7*60*1000))
			                        return;
			                }
			                else
			                {
                                Log.WarnException("Could not get current CPU usage from system, will try again later", e);
			                }
			                errorGettingCpuStats = true;
                            continue;
			            }
			            try
			            {
			                HandleCpuUsage(totalUsage);
			            }
			            catch (Exception e)
			            {
			                Log.WarnException("Failed to notify handlers about CPU usage, aborting CPU throttling", e);
			                return;
			            }
			        }
			    }
			    catch (Exception e)
			    {
			        Log.ErrorException("Errpr handling CPU statistics during automatic CPU throttling, aborting automatic thorttling!", e);
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

            nextWriteIndex = 0;

			var average = Average = LastUsages.Average();
		    if (average < 0)
		        return; // there was an error in getting the CPU stats, ignoring

			if (average >= HighNotificationThreshold)
				RunCpuUsageHandlers(handler => handler.HandleHighCpuUsage());
			else if(average < LowNotificationThreshold)
				RunCpuUsageHandlers(handler => handler.HandleLowCpuUsage());

		}

		public static void RegisterCpuUsageHandler(ICpuUsageHandler handler)
		{
		    if (dynamicLoadBalancding == false)
		        return;
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
						Log.Error("Failure to process CPU usage notification (cpu usage handler - " + handler + "), handler will be removed", e);
                        inactiveHandlers.Add(highCpuUsageHandler);
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