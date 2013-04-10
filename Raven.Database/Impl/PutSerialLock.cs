// -----------------------------------------------------------------------
//  <copyright file="PutSerialLock.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Impl
{
	/// <summary>
	/// This is a temporary class to make it easier to work with the putSerialLock
	/// until we find a way to remove it.
	/// </summary>
	public class PutSerialLock
	{
		private readonly object lockObj = new object();

		public IDisposable Lock()
		{
			Monitor.Enter(lockObj);
			return new DisposableAction(() => Monitor.Exit(lockObj));
		}
	}
}