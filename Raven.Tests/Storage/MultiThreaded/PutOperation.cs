// -----------------------------------------------------------------------
//  <copyright file="MultiThreadedStorages.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Storage.MultiThreaded
{
	public class PutOperation : MultiThreaded
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

		protected override int SetupData()
		{
			DocumentDatabase.Put("Raven/Hilo/users", null, new RavenJObject(), new RavenJObject(), null);
			DocumentDatabase.Put("Raven/Hilo/posts", null, new RavenJObject(), new RavenJObject(), null);

			return 2;
		}
	}
}