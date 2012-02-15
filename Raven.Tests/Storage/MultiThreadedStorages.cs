// -----------------------------------------------------------------------
//  <copyright file="MultiThreadedStorages.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Xunit;

namespace Raven.Tests.Storage
{
	public class MultiThreadedStorages : MultiThreaded
	{
		[Fact]
		public void WhenUsingEsentOnDisk()
		{
			SetupDatabase(typeof(Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName, false);
			ShoudlGetEverything();
		}

		[Fact]
		public void WhenUsingEsentInMemory()
		{
			SetupDatabase(typeof(Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName, true);
			ShoudlGetEverything();
		}

		[Fact]
		public void WhenUsingMuninOnDisk()
		{
			SetupDatabase(typeof(Raven.Storage.Managed.TransactionalStorage).AssemblyQualifiedName, false);
			ShoudlGetEverything();
		}

		[Fact]
		public void WhenUsingMuninInMemory()
		{
			SetupDatabase(typeof(Raven.Storage.Managed.TransactionalStorage).AssemblyQualifiedName, true);
			ShoudlGetEverything();
		}
	}
}