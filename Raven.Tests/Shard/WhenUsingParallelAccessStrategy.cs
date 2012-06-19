//-----------------------------------------------------------------------
// <copyright file="WhenUsingParallelAccessStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Tests.Document;
using Xunit;
using System.Collections.Generic;

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

		public override void Dispose()
		{
			IOExtensions.DeleteDirectory(path);
			base.Dispose();
		}

		[Fact]
		public void NullResultIsNotAnException()
		{
			using(GetNewServer(port, path))
			using (var shard1 = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			using (var session = shard1.OpenSession())
			{
				var results = new ParallelShardAccessStrategy().Apply(new[] { shard1.DatabaseCommands }, new ShardRequestData(), (x, i) => (IList<Company>)null);

				Assert.Equal(1, results.Length);
				Assert.Null(results[0]);
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
				Assert.Throws<ApplicationException>(() => parallelShardAccessStrategy.Apply<object>(new[] {shard1.DatabaseCommands}, new ShardRequestData(), (x, i) => { throw new ApplicationException(); }));
			}
		}
	}
}