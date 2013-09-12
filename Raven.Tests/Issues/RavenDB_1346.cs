// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1346.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Issues
{
	using System;
	using System.Collections.Generic;
	using System.Linq;

	using Raven.Client;

	using Xunit;

	public class RavenDB_1346 : RavenTest
	{
		[Fact]
		public void CanGetOrCreateVoteResultCollection()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var collection = new VoteResultCollectionHelper(session).GetOrCreateVoteResultCollection("entities/1");
					Assert.NotNull(collection);
				}
			}
		}

		public class VoteResultCollectionHelper
		{
			private const int MaxEntitiesPerCollectionCount = 256;
			private readonly IDocumentSession documentSession;

			public VoteResultCollectionHelper(IDocumentSession documentSession)
			{
				this.documentSession = documentSession;
			}

			public ResultCollection GetOrCreateVoteResultCollection(string entityId)
			{
				var openCollection = documentSession.Query<ResultCollection>()
													.FirstOrDefault(x => x.ResultCollectionItems.Count < MaxEntitiesPerCollectionCount &&
																		 x.EntityId.Equals(entityId) &&
																		 x.ResultCollectionType == "sorted");
				if (openCollection == null)
				{
					var existingFullCollection = documentSession.Query<ResultCollection>()
																.FirstOrDefault(x => x.ResultCollectionItems.Count >= MaxEntitiesPerCollectionCount &&
																					 x.EntityId.Equals(entityId) &&
																					 x.ResultCollectionType == "sorted");

					var newCollection = new ResultCollection(entityId, "sorted");
					if (existingFullCollection != null)
					{
						// removed for brevity
						// some more code for moving items between an existing collection and the new one.
					}

					return newCollection;
				}

				return openCollection;
			}
		}

		public class ResultCollection
		{
			public string Id { get; set; }
			public string EntityId { get; private set; }
			public string ResultCollectionType { get; private set; }
			public DateTime Created { get; private set; }

			public List<string> ResultCollectionItems { get; set; }

			public ResultCollection()
			{
				ResultCollectionItems = new List<string>();
				Created = DateTime.Now;
			}

			public ResultCollection(string entityId, string resultCollectionType)
			{
				EntityId = entityId;
				ResultCollectionType = resultCollectionType;
				ResultCollectionItems = new List<string>();
				Created = DateTime.Now;
			}
		}
	}
}