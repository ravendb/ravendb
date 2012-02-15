// -----------------------------------------------------------------------
//  <copyright file="MultiThreadedStorages.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Commands;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Storage.MultiThreaded
{
	public class BatchOperation : MultiThreaded
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
			DocumentDatabase.Batch(new[]
			                       {
			                       	new PutCommandData
			                       	{
			                       		Document = new RavenJObject(),
			                       		Etag = null,
			                       		Key = "users/1",
			                       		Metadata = new RavenJObject(),
			                       		TransactionInformation = null
			                       	},
			                       	new PutCommandData
			                       	{
			                       		Document = new RavenJObject(),
			                       		Etag = null,
			                       		Key = "posts/1",
			                       		Metadata = new RavenJObject(),
			                       		TransactionInformation = null
			                       	},
			                       	new PutCommandData
			                       	{
			                       		Document = new RavenJObject(),
			                       		Etag = null,
			                       		Key = "posts/2",
			                       		Metadata = new RavenJObject(),
			                       		TransactionInformation = null
			                       	},
			                       	new PutCommandData
			                       	{
			                       		Document = new RavenJObject(),
			                       		Etag = null,
			                       		Key = "posts/3",
			                       		Metadata = new RavenJObject(),
			                       		TransactionInformation = null
			                       	},
			                       	new PutCommandData
			                       	{
			                       		Document = new RavenJObject(),
			                       		Etag = null,
			                       		Key = "posts/4",
			                       		Metadata = new RavenJObject(),
			                       		TransactionInformation = null
			                       	},
			                       	new PutCommandData
			                       	{
			                       		Document = new RavenJObject(),
			                       		Etag = null,
			                       		Key = "posts/5",
			                       		Metadata = new RavenJObject(),
			                       		TransactionInformation = null
			                       	},
			                       });

			return 6;
		}
	}
}