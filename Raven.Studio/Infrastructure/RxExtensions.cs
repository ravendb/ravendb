using System;
using System.Reactive;
using System.Reactive.Linq;

namespace Raven.Studio.Infrastructure
{
	public static class RxExtensions
	{
		public static IObservable<object> NoSignature<T>(this IObservable<EventPattern<T>> observable) where T : EventArgs
		{
			return observable.Select(e => (object) null);
		}
	}
}