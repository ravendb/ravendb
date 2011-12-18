//-----------------------------------------------------------------------
// <copyright file="When_Using_Multiple_Unsharded_Servers.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Xunit;
using System.Collections.Generic;

namespace Raven.Tests.Document
{
	public class When_Using_Multiple_Unsharded_Servers : RemoteClientTest, IDisposable
	{
		private readonly string path1;
		private readonly string path2;
		private readonly int port1;
		private readonly int port2;

		public When_Using_Multiple_Unsharded_Servers()
		{
			port1 = 8079;
			port2 = 8081;

			path1 = GetPath("TestUnshardedDb1");
			path2 = GetPath("TestUnshardedDb2");

			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port1);
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port2);
		}

		[Fact]
		public void Can_insert_into_two_servers_running_simultaneously_without_sharding()
		{
			using (var server1 = GetNewServer(port1, path1))
			using (var server2 = GetNewServer(port2, path2))
			{
				foreach (var port in new[] { port1, port2 })
				{
					using (var documentStore = new DocumentStore { Url = "http://localhost:"+ port }.Initialize())
					using (var session = documentStore.OpenSession())
					{
						var entity = new Company { Name = "Company" };
						session.Store(entity);
						session.SaveChanges();
						Assert.NotEqual(Guid.Empty.ToString(), entity.Id);
					}
				}
			}
		}

		#region IDisposable Members

		public void Dispose()
		{
			Thread.Sleep(100);

			foreach (var path in new[] { path1, path2 })
			{
				try
				{
					IOExtensions.DeleteDirectory(path);
				}
				catch (Exception) { }
			}
		}

		#endregion

	}
}
