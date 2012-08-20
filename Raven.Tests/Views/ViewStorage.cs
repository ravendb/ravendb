//-----------------------------------------------------------------------
// <copyright file="ViewStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.MEF;
using Raven.Database.Plugins;
using Raven.Json.Linq;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Storage;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Views
{
	public class ViewStorage : RavenTest
	{
		private readonly ITransactionalStorage transactionalStorage;

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
				actions.MapReduce.PutMappedResult("CommentCountsByBlog", "123", "1", RavenJObject.Parse("{'a': 'abc'}"));
				actions.MapReduce.PutMappedResult("CommentCountsByBlog", "324", "2", RavenJObject.Parse("{'a': 'def'}"));
				actions.MapReduce.PutMappedResult("CommentCountsByBlog", "321", "1", RavenJObject.Parse("{'a': 'ijg'}"));
			});
		}

		[Fact]
		public void CanUpdateValue()
		{
			transactionalStorage.Batch(actions => actions.MapReduce.PutMappedResult("CommentCountsByBlog", "123", "1", RavenJObject.Parse("{'a': 'abc'}")));


			transactionalStorage.Batch(actions => actions.MapReduce.PutMappedResult("CommentCountsByBlog", "123", "1", RavenJObject.Parse("{'a': 'def'}")));
		}

		[Fact]
		public void CanStoreAndGetValues()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.MapReduce.PutMappedResult("CommentCountsByBlog", "123", "1", RavenJObject.Parse("{'a': 'abc'}"));
				actions.MapReduce.PutMappedResult("CommentCountsByBlog", "324", "2", RavenJObject.Parse("{'a': 'def'}"));
				actions.MapReduce.PutMappedResult("CommentCountsByBlog", "321", "1", RavenJObject.Parse("{'a': 'ijg'}"));
			});


			transactionalStorage.Batch(actions =>
			{
				var vals = actions.MapReduce.GetMappedResultsForDebug("CommentCountsByBlog", "1",100).ToArray();
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
				actions.MapReduce.PutMappedResult("CommentCountsByBlog", "123", "1", RavenJObject.Parse("{'a': 'abc'}"));
			});


			transactionalStorage.Batch(actions =>
			{
				actions.MapReduce.PutMappedResult("CommentCountsByBlog", "123", "1", RavenJObject.Parse("{'a': 'def'}"));
			});

			transactionalStorage.Batch(actions =>
			{
				var strings = actions.MapReduce.GetMappedResultsForDebug("CommentCountsByBlog", "1", 100).Select(x => x.ToString()).ToArray();
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
				actions.MapReduce.PutMappedResult("CommentCountsByBlog", "123", "1", RavenJObject.Parse("{'a': 'abc'}"));
			});


			transactionalStorage.Batch(actions =>
			{
				actions.MapReduce.DeleteMappedResultsForDocumentId("123", "CommentCountsByBlog", new HashSet<ReduceKeyAndBucket>());
				actions.MapReduce.PutMappedResult("CommentCountsByBlog", "123", "1", RavenJObject.Parse("{'a': 'def'}"));
			});

			transactionalStorage.Batch(actions =>
			{
				var strings = actions.MapReduce.GetMappedResultsForDebug("CommentCountsByBlog", "1", 1000).Select(x => x.ToString()).ToArray();
				Assert.Contains("def", strings[0]);
			});
		}


		[Fact]
		public void CanDeleteValueByDocumentId()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.MapReduce.PutMappedResult("CommentCountsByBlog1", "123", "1", RavenJObject.Parse("{'a': 'abc'}"));
				actions.MapReduce.PutMappedResult("CommentCountsByBlog2", "123", "1", RavenJObject.Parse("{'a': 'abc'}"));
			});

			transactionalStorage.Batch(actions =>
			{
				actions.MapReduce.DeleteMappedResultsForDocumentId("123", "CommentCountsByBlog2", new HashSet<ReduceKeyAndBucket>());
				actions.MapReduce.DeleteMappedResultsForDocumentId("123", "CommentCountsByBlog1", new HashSet<ReduceKeyAndBucket>());
			});

			transactionalStorage.Batch(actions =>
			{
				Assert.Empty(actions.MapReduce.GetMappedResultsForDebug("CommentCountsByBlog1", "1",100));
				Assert.Empty(actions.MapReduce.GetMappedResultsForDebug("CommentCountsByBlog2", "1",100));
			});
		}

		[Fact]
		public void CanDeleteValueByView()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.MapReduce.PutMappedResult("CommentCountsByBlog1", "123", "1", RavenJObject.Parse("{'a': 'abc'}"));
				actions.MapReduce.PutMappedResult("CommentCountsByBlog2", "123", "1", RavenJObject.Parse("{'a': 'abc'}"));
			});


			transactionalStorage.Batch(actions =>
			{
				actions.MapReduce.DeleteMappedResultsForView("CommentCountsByBlog2");
			});

			transactionalStorage.Batch(actions =>
			{
				Assert.NotEmpty(actions.MapReduce.GetMappedResultsForDebug("CommentCountsByBlog1", "1", 100));
				Assert.Empty(actions.MapReduce.GetMappedResultsForDebug("CommentCountsByBlog2", "1", 100));
			});
		}
	}
}
