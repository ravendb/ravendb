// -----------------------------------------------------------------------
//  <copyright file="Basic.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Xunit;

namespace Raven.Tests.Raft
{
	public class Basic : RaftTestBase
	{
		[Fact]
		public void T1()
		{
			var nodes = CreateRaftCluster(3);
		}
	}
}