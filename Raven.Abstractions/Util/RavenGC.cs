// -----------------------------------------------------------------------
//  <copyright file="RavenGC.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
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

		public static void CollectGarbage(bool waitForPendingFinalizers = false)
		{
			ReleaseMemoryBeforeGC();

			GC.Collect();

			if (waitForPendingFinalizers)
				GC.WaitForPendingFinalizers();
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

		public static void CollectGarbage(int generation, GCCollectionMode collectionMode = GCCollectionMode.Default)
		{
			ReleaseMemoryBeforeGC();

			GC.Collect(generation, collectionMode);
		}

		//[MethodImpl(MethodImplOptions.Synchronized)]
		public static void CollectGarbage(bool compactLoh, Action afterCollect)
		{
			ReleaseMemoryBeforeGC();

			if (compactLoh)
				SetCompactLog.Value();

			GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);

			if (afterCollect != null)
				afterCollect();

			GC.WaitForPendingFinalizers();
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