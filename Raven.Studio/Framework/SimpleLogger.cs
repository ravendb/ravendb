namespace Raven.Studio.Framework
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;

	/// <summary>
	/// Naïve, not threadsafe logger to help with some perf issues
	/// </summary>
	public static class SimpleLogger
	{
		static readonly Dictionary<string, DateTime> tasks = new Dictionary<string, DateTime>();

		public static void Start(string task)
		{
			if (tasks.ContainsKey(task))
			{
				Debug.WriteLine(task + " already executing...");

				var started = tasks[task];
				var delta = DateTime.Now - started;
				Debug.WriteLine("- started " + delta.TotalMilliseconds + "ms ago.");
			}
			else
			{
				tasks[task] = DateTime.Now;
				Debug.WriteLine(task + " started");
			}
		}

		public static void End(string task)
		{
			if (tasks.ContainsKey(task))
			{
				var started = tasks[task];
				var delta = DateTime.Now - started;
				Debug.WriteLine(task + " end. " + delta.TotalMilliseconds + "ms");
				tasks.Remove(task);
			}
			else
			{
				Debug.WriteLine("! no start found for " + task);
			}
		}
	}
}