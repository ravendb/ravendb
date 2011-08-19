namespace Raven.Abstractions
{
	using System;

	public static class SystemTime
	{
		public static Func<DateTime> Now = () => DateTime.Now;
	}
}