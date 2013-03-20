using System.Collections.Concurrent;

namespace Raven.Database.Extensions
{
	public static class ConcurrentQueueExtensions
	{
		 public static T Peek<T>(this ConcurrentQueue<T> self)
				where T : class
		 {
			 T result;
			 self.TryPeek(out result);
			 return result;
		 }
	}
}