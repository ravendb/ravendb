// -----------------------------------------------------------------------
//  <copyright file="MultiThreadedStorages.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Commands;
using Raven.Json.Linq;

namespace Raven.StressTests.Storage.MultiThreaded
{
	public class BatchOperation : MultiThreaded
	{
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