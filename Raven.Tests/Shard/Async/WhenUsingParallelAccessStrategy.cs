//-----------------------------------------------------------------------
// <copyright file="WhenUsingParallelAccessStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Tests.Document;
using Xunit;

namespace Raven.Tests.Shard.Async
{
	public class WhenUsingParallelAccessStrategy  : RemoteClientTest
	{
		[Fact]
		public void NullResultIsNotAnException()
		{
			using(GetNewServer())
			using (var shard1 = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			using (var session = shard1.OpenAsyncSession())
			{
				var results = new ParallelShardAccessStrategy().ApplyAsync(new[] { shard1.AsyncDatabaseCommands },
					new ShardRequestData(), (x, i) => CompletedTask.With((IList<Company>)null).Task).Result;

				Assert.Equal(1, results.Length);
				Assert.Null(results[0]);
			}
		}

		[Fact]
		public void ExecutionExceptionsAreRethrown()
		{
			using (GetNewServer())
			using (var shard1 = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			using (var session = shard1.OpenAsyncSession())
			{
				var parallelShardAccessStrategy = new ParallelShardAccessStrategy();
				Assert.Throws<ApplicationException>(() => parallelShardAccessStrategy.ApplyAsync<object>(new[] { shard1.AsyncDatabaseCommands },
					new ShardRequestData(), (x, i) => { throw new ApplicationException(); }).Wait());
			}
		}
	}
}