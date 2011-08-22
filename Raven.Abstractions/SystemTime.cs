namespace Raven.Abstractions
{
	using System;

	public static class SystemTime
	{
		public static DateTime Now { get { return UtcDateTime().ToLocalTime(); } }
		public static DateTime UtcNow { get { return UtcDateTime(); } }
		public static Func<DateTime> UtcDateTime = () => DateTime.UtcNow;
	}
}