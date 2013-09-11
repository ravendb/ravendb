// -----------------------------------------------------------------------
//  <copyright file="IAsyncEnumerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;

namespace Raven.Abstractions.Util
{
	public interface IAsyncEnumerator<T> : IDisposable
	{
		Task<bool> MoveNextAsync();
		T Current { get; }
	}
}