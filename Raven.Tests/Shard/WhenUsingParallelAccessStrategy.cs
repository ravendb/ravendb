//-----------------------------------------------------------------------
// <copyright file="WhenUsingParallelAccessStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Tests.Document;
using Xunit;
using System.Collections.Generic;
using Raven.Client.Shard.ShardStrategy.ShardAccess;

namespace Raven.Tests.Shard
{
	public class WhenUsingParallelAccessStrategy  : RemoteClientTest, IDisposable
	{
		private readonly string path;
		private readonly int port;

		public WhenUsingParallelAccessStrategy()
		{
			port = 8079;
			path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8079);
		}

		public void Dispose()
		{
			IOExtensions.DeleteDirectory(path);
		}

		[Fact]
		public void NullResultIsNotAnException()
		{
			using(GetNewServer(port, path))
			using (var shard1 = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			using (var session = shard1.OpenSession())
			{
				var results = new ParallelShardAccessStrategy().Apply(new[] { session.Advanced.DatabaseCommands }, (x, i) => (IList<Company>)null);

				Assert.Equal(0, results.Count);
			}
		}

		[Fact]
		public void ExecutionExceptionsAreRethrown()
		{
			using (GetNewServer(port, path))
			using (var shard1 = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			using (var session = shard1.OpenSession())
			{
				var parallelShardAccessStrategy = new ParallelShardAccessStrategy();
				Assert.Throws<ApplicationException>(() => parallelShardAccessStrategy.Apply<object>(new[] {session.Advanced.DatabaseCommands}, (x, i) => { throw new ApplicationException(); }));
			}
		}
	}
}