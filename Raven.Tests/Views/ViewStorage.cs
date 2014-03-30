//-----------------------------------------------------------------------
// <copyright file="ViewStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Json.Linq;
using Raven.Database.Storage;
using Xunit;

namespace Raven.Tests.Views
{
	public class ViewStorage : RavenTest
	{
		private readonly ITransactionalStorage transactionalStorage;
	    private int id = 100;
	    private int one = 200;
	    private int two = 300;

		public ViewStorage()
		{
			transactionalStorage = NewTransactionalStorage();
		}

		public override void Dispose()
		{
			transactionalStorage.Dispose();
			base.Dispose();
		}


		[Fact]
		public void CanStoreValues()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.MapReduce.PutMappedResult(id, "123", "1", RavenJObject.Parse("{'a': 'abc'}"));
				actions.MapReduce.PutMappedResult(id, "324", "2", RavenJObject.Parse("{'a': 'def'}"));
				actions.MapReduce.PutMappedResult(id, "321", "1", RavenJObject.Parse("{'a': 'ijg'}"));
			});
		}

		[Fact]
		public void CanUpdateValue()
		{
			transactionalStorage.Batch(actions => actions.MapReduce.PutMappedResult(id, "123", "1", RavenJObject.Parse("{'a': 'abc'}")));
			transactionalStorage.Batch(actions => actions.MapReduce.PutMappedResult(id, "123", "1", RavenJObject.Parse("{'a': 'def'}")));
		}

		[Fact]
		public void CanStoreAndGetValues()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.MapReduce.PutMappedResult(id, "123", "1", RavenJObject.Parse("{'a': 'abc'}"));
				actions.MapReduce.PutMappedResult(id, "324", "2", RavenJObject.Parse("{'a': 'def'}"));
				actions.MapReduce.PutMappedResult(id, "321", "1", RavenJObject.Parse("{'a': 'ijg'}"));
			});


			transactionalStorage.Batch(actions =>
			{
				var vals = actions.MapReduce.GetMappedResultsForDebug(id, "1", 0, 100).ToArray();
				Assert.Equal(2, vals.Length);
				Assert.Contains("abc", vals[0].ToString());
				Assert.Contains("ijg", vals[1].ToString());
			});
		}


		[Fact]
		public void CanAddmultipleValuesForTheSameKey()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.MapReduce.PutMappedResult(id, "123", "1", RavenJObject.Parse("{'a': 'abc'}"));
			});


			transactionalStorage.Batch(actions =>
			{
				actions.MapReduce.PutMappedResult(id, "123", "1", RavenJObject.Parse("{'a': 'def'}"));
			});

			transactionalStorage.Batch(actions =>
			{
				var strings = actions.MapReduce.GetMappedResultsForDebug(id, "1", 0, 100).Select(x => x.ToString()).ToArray();
				Assert.Equal(2, strings.Length);
				Assert.Contains("abc", strings[0]);
				Assert.Contains("def", strings[1]);
			});
		}

		[Fact]
		public void CanUpdateValueAndGetUpdatedValues()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.MapReduce.PutMappedResult(id, "123", "1", RavenJObject.Parse("{'a': 'abc'}"));
			});


			transactionalStorage.Batch(actions =>
			{
				actions.MapReduce.DeleteMappedResultsForDocumentId("123",id, new Dictionary<ReduceKeyAndBucket, int>());
				actions.MapReduce.PutMappedResult(id, "123", "1", RavenJObject.Parse("{'a': 'def'}"));
			});

			transactionalStorage.Batch(actions =>
			{
				var strings = actions.MapReduce.GetMappedResultsForDebug(id, "1", 0, 1000).Select(x => x.ToString()).ToArray();
				Assert.Contains("def", strings[0]);
			});
		}


		[Fact]
		public void CanDeleteValueByDocumentId()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.MapReduce.PutMappedResult(one, "123", "1", RavenJObject.Parse("{'a': 'abc'}"));
				actions.MapReduce.PutMappedResult(two, "123", "1", RavenJObject.Parse("{'a': 'abc'}"));
			});

			transactionalStorage.Batch(actions =>
			{
				actions.MapReduce.DeleteMappedResultsForDocumentId("123",two, new Dictionary<ReduceKeyAndBucket, int>());
				actions.MapReduce.DeleteMappedResultsForDocumentId("123",one, new Dictionary<ReduceKeyAndBucket, int>());
			});

			transactionalStorage.Batch(actions =>
			{
				Assert.Empty(actions.MapReduce.GetMappedResultsForDebug(one, "1",0,100));
				Assert.Empty(actions.MapReduce.GetMappedResultsForDebug(two, "1", 0, 100));
			});
		}

		[Fact]
		public void CanDeleteValueByView()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.MapReduce.PutMappedResult(one, "123", "1", RavenJObject.Parse("{'a': 'abc'}"));
				actions.MapReduce.PutMappedResult(two, "123", "1", RavenJObject.Parse("{'a': 'abc'}"));
			});


			transactionalStorage.Batch(actions =>
			{
				actions.MapReduce.DeleteMappedResultsForView(two);
			});

			transactionalStorage.Batch(actions =>
			{
				Assert.NotEmpty(actions.MapReduce.GetMappedResultsForDebug(one, "1", 0, 100));
				Assert.Empty(actions.MapReduce.GetMappedResultsForDebug(two, "1", 0, 100));
			});
		}
	}
}