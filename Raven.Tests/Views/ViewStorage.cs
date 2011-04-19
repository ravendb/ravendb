//-----------------------------------------------------------------------
// <copyright file="ViewStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Storage;
using Raven.Storage.Esent;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Views
{
	public class ViewStorage : AbstractDocumentStorageTest
	{
		private readonly ITransactionalStorage transactionalStorage;

		public ViewStorage()
		{
			transactionalStorage = new TransactionalStorage(new RavenConfiguration
			{
				DataDirectory = "raven.db.test.esent",
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
			}, () => { });
			transactionalStorage.Initialize(new DummyUuidGenerator());
		}

		#region IDisposable Members

		public override void Dispose()
		{
			transactionalStorage.Dispose();
			base.Dispose();
		}

		#endregion

		[Fact]
		public void CanStoreValues()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.MappedResults.PutMappedResult("CommentCountsByBlog", "123", "1", RavenJObject.Parse("{'a': 'abc'}"), MapReduceIndex.ComputeHash("CommentCountsByBlog", "1"));
				actions.MappedResults.PutMappedResult("CommentCountsByBlog", "324", "2", RavenJObject.Parse("{'a': 'def'}"), MapReduceIndex.ComputeHash("CommentCountsByBlog", "2"));
				actions.MappedResults.PutMappedResult("CommentCountsByBlog", "321", "1", RavenJObject.Parse("{'a': 'ijg'}"), MapReduceIndex.ComputeHash("CommentCountsByBlog", "1"));
			});
		}

		[Fact]
		public void CanUpdateValue()
		{
			transactionalStorage.Batch(actions => actions.MappedResults.PutMappedResult("CommentCountsByBlog", "123", "1", RavenJObject.Parse("{'a': 'abc'}"), MapReduceIndex.ComputeHash("CommentCountsByBlog", "1")));


			transactionalStorage.Batch(actions => actions.MappedResults.PutMappedResult("CommentCountsByBlog", "123", "1", RavenJObject.Parse("{'a': 'def'}"), MapReduceIndex.ComputeHash("CommentCountsByBlog", "1")));
		}

		[Fact]
		public void CanStoreAndGetValues()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.MappedResults.PutMappedResult("CommentCountsByBlog", "123", "1", RavenJObject.Parse("{'a': 'abc'}"), MapReduceIndex.ComputeHash("CommentCountsByBlog", "1"));
				actions.MappedResults.PutMappedResult("CommentCountsByBlog", "324", "2", RavenJObject.Parse("{'a': 'def'}"), MapReduceIndex.ComputeHash("CommentCountsByBlog", "2"));
				actions.MappedResults.PutMappedResult("CommentCountsByBlog", "321", "1", RavenJObject.Parse("{'a': 'ijg'}"), MapReduceIndex.ComputeHash("CommentCountsByBlog", "1"));
			});


			transactionalStorage.Batch(actions =>
			{
				var vals = actions.MappedResults.GetMappedResults(new GetMappedResultsParams("CommentCountsByBlog", "1", MapReduceIndex.ComputeHash("CommentCountsByBlog", "1"))).ToArray();
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
				actions.MappedResults.PutMappedResult("CommentCountsByBlog", "123", "1", RavenJObject.Parse("{'a': 'abc'}"), MapReduceIndex.ComputeHash("CommentCountsByBlog", "1"));
			});


			transactionalStorage.Batch(actions =>
			{
				actions.MappedResults.PutMappedResult("CommentCountsByBlog", "123", "1", RavenJObject.Parse("{'a': 'def'}"), MapReduceIndex.ComputeHash("CommentCountsByBlog", "1"));
			});

			transactionalStorage.Batch(actions =>
			{
				var strings = actions.MappedResults.GetMappedResults(new GetMappedResultsParams("CommentCountsByBlog", "1", MapReduceIndex.ComputeHash("CommentCountsByBlog", "1"))).Select(x => x.ToString()).ToArray();
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
                actions.MappedResults.PutMappedResult("CommentCountsByBlog", "123", "1", RavenJObject.Parse("{'a': 'abc'}"), MapReduceIndex.ComputeHash("CommentCountsByBlog", "1"));
            });


            transactionalStorage.Batch(actions =>
            {
                actions.MappedResults.DeleteMappedResultsForDocumentId("123", "CommentCountsByBlog");
				actions.MappedResults.PutMappedResult("CommentCountsByBlog", "123", "1", RavenJObject.Parse("{'a': 'def'}"), MapReduceIndex.ComputeHash("CommentCountsByBlog", "1"));
            });

            transactionalStorage.Batch(actions =>
            {
                var strings = actions.MappedResults.GetMappedResults(new GetMappedResultsParams("CommentCountsByBlog", "1", MapReduceIndex.ComputeHash("CommentCountsByBlog", "1"))).Select(x => x.ToString()).ToArray();
                Assert.Contains("def", strings[0]);
            });
        }


		[Fact]
		public void CanDeleteValueByDocumentId()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.MappedResults.PutMappedResult("CommentCountsByBlog1", "123", "1", RavenJObject.Parse("{'a': 'abc'}"), MapReduceIndex.ComputeHash("CommentCountsByBlog1", "1"));
				actions.MappedResults.PutMappedResult("CommentCountsByBlog2", "123", "1", RavenJObject.Parse("{'a': 'abc'}"), MapReduceIndex.ComputeHash("CommentCountsByBlog2", "1"));
			});

			transactionalStorage.Batch(actions =>
			{
				actions.MappedResults.DeleteMappedResultsForDocumentId("123", "CommentCountsByBlog2");
				actions.MappedResults.DeleteMappedResultsForDocumentId("123", "CommentCountsByBlog1");
			});

			transactionalStorage.Batch(actions =>
			{
				Assert.Empty(actions.MappedResults.GetMappedResults(new GetMappedResultsParams("CommentCountsByBlog1", "1", MapReduceIndex.ComputeHash("CommentCountsByBlog1", "1"))));
				Assert.Empty(actions.MappedResults.GetMappedResults(new GetMappedResultsParams("CommentCountsByBlog2", "1", MapReduceIndex.ComputeHash("CommentCountsByBlog2", "1"))));
			});
		}

		[Fact]
		public void CanDeleteValueByView()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.MappedResults.PutMappedResult("CommentCountsByBlog1", "123", "1", RavenJObject.Parse("{'a': 'abc'}"), MapReduceIndex.ComputeHash("CommentCountsByBlog1", "1"));
				actions.MappedResults.PutMappedResult("CommentCountsByBlog2", "123", "1", RavenJObject.Parse("{'a': 'abc'}"), MapReduceIndex.ComputeHash("CommentCountsByBlog2", "1"));
			});


			transactionalStorage.Batch(actions =>
			{
				actions.MappedResults.DeleteMappedResultsForView("CommentCountsByBlog2");
			});

			transactionalStorage.Batch(actions =>
			{
				Assert.NotEmpty(actions.MappedResults.GetMappedResults(new GetMappedResultsParams("CommentCountsByBlog1", "1", MapReduceIndex.ComputeHash("CommentCountsByBlog1", "1"))));
				Assert.Empty(actions.MappedResults.GetMappedResults(new GetMappedResultsParams("CommentCountsByBlog2", "1", MapReduceIndex.ComputeHash("CommentCountsByBlog2", "1"))));
			});
		}
	}
}
