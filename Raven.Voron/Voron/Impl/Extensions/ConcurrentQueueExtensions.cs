namespace Voron.Impl.Extensions
{
	using System.Collections.Concurrent;

	public static class ConcurrentQueueExtensions
	{
		public static T Peek<T>(this ConcurrentQueue<T> self)
			where T : class
		{
			T result;
			if (self.TryPeek(out result) == false)
				return null;
			return result;
		}
	}
}