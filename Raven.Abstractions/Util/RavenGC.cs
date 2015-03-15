// -----------------------------------------------------------------------
//  <copyright file="RavenGC.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Raven.Abstractions.Logging;
using Raven.Database.Util;

namespace Raven.Abstractions.Util
{
	using System;
	using System.Linq.Expressions;
	using System.Runtime;

	public static class RavenGC
	{
		private static readonly ConcurrentSet<WeakReference<Action>> _releaseMemoryBeforeGC = new ConcurrentSet<WeakReference<Action>>();
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		private static long memoryBeforeLastForcedGC;
		private static long memoryAfterLastForcedGC;

		private static DateTime lastForcedGCTime;

		private static int delayBetweenGCInMinutes;
		private const int DefaultDelayBetweenGCInMinutes = 1;
		private const int MaxDelayBetweenGCInMinutes = 60;
		static RavenGC()
		{
			lastForcedGCTime = DateTime.MinValue;
			memoryAfterLastForcedGC = 0;
			memoryBeforeLastForcedGC = 0;
			delayBetweenGCInMinutes = DefaultDelayBetweenGCInMinutes;
		}

		public static long MemoryBeforeLastForcedGC
		{
			get { return memoryBeforeLastForcedGC; }
		}
		public static long MemoryAfterLastForcedGC
		{
			get { return memoryAfterLastForcedGC; }
		}
		public static DateTime LastForcedGCTime
		{
			get { return lastForcedGCTime; }
		}

		public static void Register(Action action)
		{
			_releaseMemoryBeforeGC.Add(new WeakReference<Action>(action));
		}

		public static void Unregister(Action action)
		{
			_releaseMemoryBeforeGC.RemoveWhere(reference =>
			{
				Action target;
				return reference.TryGetTarget(out target) == false || target == action;
			});
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public static bool CollectGarbage(bool waitForPendingFinalizers = false, bool isAdmin = false)
		{
			if (ShouldCollectNow() || isAdmin)
			{
				ReleaseMemoryBeforeGC();

				CalculateMemoryBefore();
				GC.Collect();

				if (waitForPendingFinalizers)
					GC.WaitForPendingFinalizers();

				CalculateMemoryAfter();				
				lastForcedGCTime = DateTime.UtcNow;
				delayBetweenGCInMinutes = DefaultDelayBetweenGCInMinutes;
				return true;
			}

			return false;
		}

		private static void ReleaseMemoryBeforeGC()
		{
			var inactiveHandlers = new List<WeakReference<Action>>();

			foreach (var lowMemoryHandler in _releaseMemoryBeforeGC)
			{
				Action handler;
				if (lowMemoryHandler.TryGetTarget(out handler))
				{
					try
					{
						handler();
					}
					catch (Exception e)
					{
						log.Error("Failure to process release memory before gc, skipping", e);
					}
				}
				else
					inactiveHandlers.Add(lowMemoryHandler);
			}

			inactiveHandlers.ForEach(x => _releaseMemoryBeforeGC.TryRemove(x));
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public static bool CollectGarbage(int generation, GCCollectionMode collectionMode = GCCollectionMode.Default, bool isAdmin = false)
		{
			if (ShouldCollectNow() || isAdmin)
			{
				ReleaseMemoryBeforeGC();
				CalculateMemoryBefore();
				GC.Collect(generation, collectionMode);
				CalculateMemoryAfter();
				log.Debug("Finished GC, before was {0}kb, after is {1}kb", MemoryBeforeLastForcedGC, MemoryAfterLastForcedGC);

				lastForcedGCTime = DateTime.UtcNow;
				delayBetweenGCInMinutes = DefaultDelayBetweenGCInMinutes;
				return true;
			}

			return false;
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public static bool CollectGarbage(bool compactLoh, Action afterCollect, bool isAdmin = false)
		{
			if (ShouldCollectNow() || isAdmin)
			{
				ReleaseMemoryBeforeGC();
				if (compactLoh)
					SetCompactLog.Value();

				CalculateMemoryBefore();
				GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
				if (afterCollect != null)
					afterCollect();

				GC.WaitForPendingFinalizers();
				CalculateMemoryAfter();
				log.Debug("Finished GC, before was {0}kb, after is {1}kb", MemoryBeforeLastForcedGC, MemoryAfterLastForcedGC);

				lastForcedGCTime = DateTime.UtcNow;
				delayBetweenGCInMinutes = DefaultDelayBetweenGCInMinutes;

				return true;
			}

			return false;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private static void CalculateMemoryBefore()
		{
			memoryBeforeLastForcedGC = GC.GetTotalMemory(false);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private static void CalculateMemoryAfter()
		{
			memoryAfterLastForcedGC = GC.GetTotalMemory(false); 
		}

		private static bool ShouldCollectNow()
		{
			var nowTime = DateTime.UtcNow;
			if (MemoryAfterLastForcedGC == 0 || MemoryBeforeLastForcedGC == 0) //running for the first time
			{
				log.Debug("GCing for the first time...");
				return true;
			}

			//if last time was freed enough memory (more than 10%) allow the GC and store last GC time
			if (DifferenceAsDecimalPercents(MemoryBeforeLastForcedGC, MemoryAfterLastForcedGC) >= 0.1)
			{
				log.Debug("Allowing GC because difference of memory before and after GC equals or more than 10% - last time was released {0}kbs.", Math.Abs(MemoryAfterLastForcedGC - MemoryBeforeLastForcedGC)/1024);
				delayBetweenGCInMinutes = DefaultDelayBetweenGCInMinutes;
				
				return true;
			}
			
			//if last time not enough memory was freed, but enough time passed since last allowed GC,
			//reset delay and allow GC
			if ((nowTime - LastForcedGCTime).TotalMinutes >= delayBetweenGCInMinutes)
			{
				log.Debug("Allowing GC because more than {1} minutes passed since last GC - last time was released {0}kbs.", Math.Abs(MemoryAfterLastForcedGC - MemoryBeforeLastForcedGC) / 1024, (nowTime - LastForcedGCTime).TotalMinutes);
				delayBetweenGCInMinutes = DefaultDelayBetweenGCInMinutes;

				return true;
			}

			//not enough memory was freed the last time, and not enough time passed
			// -> reset last time, increase delay threshold and disallow GC (too early!)
			lastForcedGCTime = nowTime;
			
			delayBetweenGCInMinutes = Math.Max(5 + delayBetweenGCInMinutes, MaxDelayBetweenGCInMinutes);

			log.Debug("Disallowing GC (not enough memory released last time and not enough time passed since last GC). New interval between GCs will be {0}min",delayBetweenGCInMinutes);
			return false;
		}

		private static double DifferenceAsDecimalPercents(long v1, long v2)
		{
			double x1 = v1;
			double x2 = v2;
			
			if (x1 > 0.0 && x2 > 0.0)
				return Math.Abs(x1 - x2)/x2;

			return 0.0;			
		}

		// this is just the code below, but we have to run on 4.5, not just 4.5.1
		// GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
		private static readonly Lazy<Action> SetCompactLog = new Lazy<Action>(() =>
		{
			var prop = typeof(GCSettings).GetProperty("LargeObjectHeapCompactionMode");
			if (prop == null)
				return (() => { });
			var enumType = Type.GetType("System.Runtime.GCLargeObjectHeapCompactionMode, mscorlib");
			var value = Enum.Parse(enumType, "CompactOnce");
			var lambda = Expression.Lambda<Action>(Expression.Assign(Expression.MakeMemberAccess(null, prop), Expression.Constant(value)));
			return lambda.Compile();
		});
		
	}
}