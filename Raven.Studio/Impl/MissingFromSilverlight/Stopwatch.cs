namespace System.Diagnostics
{
	using System;

	public class Stopwatch
	{
		readonly DateTime started;

		public Stopwatch() { started = DateTime.Now; }

		public double ElapsedMilliseconds { get { return (DateTime.Now - started).TotalMilliseconds; } }
		public static Stopwatch StartNew() { return new Stopwatch(); }
	}
}