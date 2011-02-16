//-----------------------------------------------------------------------
// <copyright file="QueryRunnerManager.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Security;
using Raven.Database.Linq;
using Raven.Database.Storage;

namespace Raven.Database.Queries.LinearQueries
{
	public class QueryRunnerManager : MarshalByRefObject
	{
		private readonly ConcurrentDictionary<string, AbstractViewGenerator> queryCache =
			new ConcurrentDictionary<string, AbstractViewGenerator>();

		[SecurityCritical]
		public override object InitializeLifetimeService()
		{
			return null;
		}

		public int QueryCacheSize
		{
			get { return queryCache.Count; }
		}

		public IRemoteSingleQueryRunner CreateSingleQueryRunner(Type remoteStorageType, object state)
		{
			var remoteStorage = (IRemoteStorage)Activator.CreateInstance(remoteStorageType, state);
			return new SingleQueryRunner(remoteStorage, queryCache);
		}
	}
}
