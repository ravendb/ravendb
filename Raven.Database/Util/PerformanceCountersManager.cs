// -----------------------------------------------------------------------
//  <copyright file="PerformanceCountersManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security;
using Raven.Abstractions.Logging;

namespace Raven.Database.Util
{
    public class PerformanceCountersManager : IDisposable
    {
        private static readonly ILog log = LogManager.GetCurrentClassLogger();

        /// <summary>
        /// The performance counter category name for RavenDB counters.
        /// </summary>
        public const string CategoryName = "RavenDB 2.0";

        public PerformanceCountersManager()
        {
            UsePerformanceCounters = true;
        }

        public bool UsePerformanceCounters { get; set; }

        public PerformanceCounter DocsPerSecCounter { get; private set; }
        public PerformanceCounter IndexedPerSecCounter { get; private set; }
        public PerformanceCounter ReducedPerSecCounter { get; private set; }
        public PerformanceCounter RequestsPerSecCounter { get; private set; }
        public PerformanceCounter ConcurrentRequestsCounter { get; private set; }

        public void Setup(string name)
        {
            try
            {
                SetupPerformanceCounter(GetPerformanceCounterName(name));
            }
            catch (UnauthorizedAccessException e)
            {
                log.WarnException("Could not setup performance counters properly because of access permissions, perf counters will not be used", e);
                UsePerformanceCounters = false;
            }
            catch (SecurityException e)
            {
                log.WarnException("Could not setup performance counters properly because of access permissions, perf counters will not be used", e);
                UsePerformanceCounters = false;
            }
        }

        private void SetupPerformanceCounter(string name)
        {
            var instances = new Dictionary<string, PerformanceCounterType>
			{
				{"# docs / sec", PerformanceCounterType.RateOfCountsPerSecond32},
				{"# docs indexed / sec", PerformanceCounterType.RateOfCountsPerSecond32}, 
				{"# docs reduced / sec", PerformanceCounterType.RateOfCountsPerSecond32},
				{"# req / sec", PerformanceCounterType.RateOfCountsPerSecond32}, 
				{"# of concurrent requests", PerformanceCounterType.NumberOfItems32}
			};

            if (IsValidCategory(instances, name) == false)
            {
                var counterCreationDataCollection = new CounterCreationDataCollection();
                foreach (var instance in instances)
                {
                    counterCreationDataCollection.Add(new CounterCreationData
                    {
                        CounterName = instance.Key,
                        CounterType = instance.Value
                    });
                }

                PerformanceCounterCategory.Create(CategoryName, "RavenDB Performance Counters", PerformanceCounterCategoryType.MultiInstance, counterCreationDataCollection);
                PerformanceCounter.CloseSharedResources(); // http://blog.dezfowler.com/2007/08/net-performance-counter-problems.html
            }

            DocsPerSecCounter = new PerformanceCounter(CategoryName, "# docs / sec", name, false);
            IndexedPerSecCounter = new PerformanceCounter(CategoryName, "# docs indexed / sec", name, false);
            ReducedPerSecCounter = new PerformanceCounter(CategoryName, "# docs reduced / sec", name, false);
            RequestsPerSecCounter = new PerformanceCounter(CategoryName, "# req / sec", name, false);
            ConcurrentRequestsCounter = new PerformanceCounter(CategoryName, "# of concurrent requests", name, false);
        }

        private bool IsValidCategory(Dictionary<string, PerformanceCounterType> instances, string instanceName)
        {
            if (PerformanceCounterCategory.Exists(CategoryName) == false)
                return false;

            foreach (var performanceCounterType in instances)
            {
                try
                {
                    new PerformanceCounter(CategoryName, performanceCounterType.Key, instanceName, readOnly: true).Dispose();
                }
                catch (Exception)
                {
                    PerformanceCounterCategory.Delete(CategoryName);
                    return false;
                }
            }
            return true;
        }

        public float RequestsPerSecond
        {
            get
            {
                if (UsePerformanceCounters == false)
                    return -1;
                return RequestsPerSecCounter.NextValue();
            }
        }

        public int ConcurrentRequests
        {
            get
            {
                if (UsePerformanceCounters == false)
                    return -1;
                return (int)ConcurrentRequestsCounter.NextValue();
            }
        }

        public void DocsPerSecIncreaseBy(int numOfDocs)
        {
            if (UsePerformanceCounters)
            {
                DocsPerSecCounter.IncrementBy(numOfDocs);
            }
        }
        public void IndexedPerSecIncreaseBy(int numOfDocs)
        {
            if (UsePerformanceCounters)
            {
                IndexedPerSecCounter.IncrementBy(numOfDocs);
            }
        }
        public void ReducedPerSecIncreaseBy(int numOfDocs)
        {
            if (UsePerformanceCounters)
            {
                ReducedPerSecCounter.IncrementBy(numOfDocs);
            }
        }

        public void IncrementRequestsPerSecCounter()
        {
            if (UsePerformanceCounters)
            {
                RequestsPerSecCounter.Increment();
            }
        }

        public void IncrementConcurrentRequestsCounter()
        {
            if (UsePerformanceCounters)
            {
                ConcurrentRequestsCounter.Increment();
            }
        }

        public void DecrementConcurrentRequestsCounter()
        {
            if (UsePerformanceCounters)
            {
                ConcurrentRequestsCounter.Decrement();
            }
        }

        private string GetPerformanceCounterName(string name)
        {
            // dealing with names who are very long (there is a limit of 80 chars for counter name)
            return name.Length > 70 ? name.Remove(70) : name;
        }

        public void Dispose()
        {
            if (DocsPerSecCounter != null)
                DocsPerSecCounter.Dispose();
            if (ReducedPerSecCounter != null)
                ReducedPerSecCounter.Dispose();
            if (RequestsPerSecCounter != null)
                RequestsPerSecCounter.Dispose();
            if (ConcurrentRequestsCounter != null)
                ConcurrentRequestsCounter.Dispose();
            if (IndexedPerSecCounter != null)
                IndexedPerSecCounter.Dispose();
        }
    }
}