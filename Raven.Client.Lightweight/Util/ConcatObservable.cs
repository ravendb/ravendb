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
	internal class ConcatObservable<T> : IObservable<T>
	{
		private readonly IObservable<T>[] inner;

		public ConcatObservable(IEnumerable<IObservable<T>> inner)
		{
			this.inner = inner.ToArray();
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