#if !SILVERLIGHT
// -----------------------------------------------------------------------
//  <copyright file="RavenGC.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Abstractions.Util
{
	using System;
	using System.Linq.Expressions;
	using System.Runtime;

	public static class RavenGC
	{
		public static void CollectGarbage(bool waitForPendingFinalizers = false)
		{
			GC.Collect();

			if (waitForPendingFinalizers)
				GC.WaitForPendingFinalizers();
		}

		public static void CollectGarbage(int generation, GCCollectionMode collectionMode = GCCollectionMode.Default)
		{
			GC.Collect(generation, collectionMode);
		}

		public static void CollectGarbage(bool compactLoh, Action afterCollect)
		{
			GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);

			if (compactLoh)
				SetCompactLog.Value();

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
#endif