// -----------------------------------------------------------------------
//  <copyright file="a.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Util
{
	internal class PushyObservable<T> : IObservable<T>
	{
		public IObservable<T> Inner { get; set; }
		private List<IObserver<T>> observers = new List<IObserver<T>>();

		public void ForceError(Exception e)
		{
			foreach (var observer in observers)
			{
				observer.OnError(e);
			}
		}

		public IDisposable Subscribe(IObserver<T> observer)
		{
			var dis = Inner.Subscribe(observer);
			observers.Add(observer);

			return new DisposableAction(() =>
			{
				observers.Remove(observer);
				dis.Dispose();
			});
		}
	}
	internal class ConcatObservable<T> : IObservable<T>
	{
		private readonly IObservable<T>[] inner;

		public ConcatObservable(IObservable<T>[] inner)
		{
			this.inner = inner;
		}

		public IDisposable Subscribe(IObserver<T> observer)
		{
			var disposables = inner.Select(x => x.Subscribe(observer)).ToArray();
			return new DisposableAction(() =>
			{
				foreach (var disposable in disposables)
				{
					disposable.Dispose();
				}
			});
		}
	}

}