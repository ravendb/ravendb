using System;

namespace Raven.Studio.Infrastructure
{
	public class WeakReference<T> where T : class
	{
		private readonly WeakReference inner;

		public WeakReference(T target)
			: this(target, false)
		{
		}

		public WeakReference(T target, bool trackResurrection)
		{
			if (target == null)
				throw new ArgumentNullException("target");

			inner = new WeakReference(target, trackResurrection);
		}

		public T Target
		{
			get { return (T) inner.Target; }
			set { inner.Target = value; }
		}

		public bool IsAlive
		{
			get { return inner.IsAlive; }
		}
	}
}