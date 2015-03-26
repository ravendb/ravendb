// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3286.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3286 : RavenTest
	{
		[Fact]
		public void T1()
		{
			using (var store = NewRemoteDocumentStore())
			{
				while (true)
				{
					store.DatabaseCommands.Get("key/1");
					Thread.Sleep(1000);
				}
			}
		}
	}
}