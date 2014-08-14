// -----------------------------------------------------------------------
//  <copyright file="PerformanceCountersMonitoring.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Linq;

namespace Raven.Database.Util
{
	public static class PerformanceCountersUtils
	{
		public static long? SafelyGetPerformanceCounter(string categoryName, string counterName, string processName)
		{
			try
			{
				if (PerformanceCounterCategory.Exists(categoryName) == false)
					return null;
				var category = new PerformanceCounterCategory(categoryName);
				var instances = category.GetInstanceNames();
				var ravenInstance = instances.FirstOrDefault(x => x == processName);
				if (ravenInstance == null || !category.CounterExists(counterName))
				{
					return null;
				}
				using (var counter = new PerformanceCounter(categoryName, counterName, ravenInstance, readOnly: true))
				{
					return counter.NextSample().RawValue;
				}
			}
			catch (Exception)
			{
				//Don't log anything here, it's up to the calling code to decide what to do
				return null;
			}
		}
	}
}