using System;
using System.Linq;
using System.Threading;
using Raven.Database;
using Raven.Database.Indexing;
using Raven.Database.Storage;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Views
{
	public class ViewStorage : AbstractDocumentStorageTest
	{
		private readonly TransactionalStorage transactionalStorage;

		public ViewStorage()
		{
			transactionalStorage = new TransactionalStorage(new RavenConfiguration
			{
				DataDirectory = "raven.db.test.esent",
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
			}, () => { });
			transactionalStorage.Initialize();
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
				actions.PutMappedResult("CommentCountsByBlog", "123", "1", "{'a': 'abc'}", MapReduceIndex.ComputeHash("CommentCountsByBlog", "1"));
				actions.PutMappedResult("CommentCountsByBlog", "324", "2", "{'a': 'def'}", MapReduceIndex.ComputeHash("CommentCountsByBlog", "2"));
				actions.PutMappedResult("CommentCountsByBlog", "321", "1", "{'a': 'ijg'}", MapReduceIndex.ComputeHash("CommentCountsByBlog", "1"));
			});
		}

		[Fact]
		public void CanUpdateValue()
		{
			transactionalStorage.Batch(actions => actions.PutMappedResult("CommentCountsByBlog", "123", "1", "{'a': 'abc'}", MapReduceIndex.ComputeHash("CommentCountsByBlog", "1")));


			transactionalStorage.Batch(actions => actions.PutMappedResult("CommentCountsByBlog", "123", "1", "{'a': 'def'}", MapReduceIndex.ComputeHash("CommentCountsByBlog", "1")));
		}

		[Fact]
		public void CanStoreAndGetValues()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.PutMappedResult("CommentCountsByBlog", "123", "1", "{'a': 'abc'}", MapReduceIndex.ComputeHash("CommentCountsByBlog", "1"));
				actions.PutMappedResult("CommentCountsByBlog", "324", "2", "{'a': 'def'}", MapReduceIndex.ComputeHash("CommentCountsByBlog", "2"));
				actions.PutMappedResult("CommentCountsByBlog", "321", "1", "{'a': 'ijg'}", MapReduceIndex.ComputeHash("CommentCountsByBlog", "1"));
			});


			transactionalStorage.Batch(actions =>
			{
				var vals = actions.GetMappedResults("CommentCountsByBlog", "1", MapReduceIndex.ComputeHash("CommentCountsByBlog", "1")).ToArray();
				Assert.Equal(2, vals.Length);
				Assert.Contains("abc", vals[0].ToString());
				Assert.Contains("ijg", vals[1].ToString());
			});
		}


		[Fact]
		public void CanUpdateValueAndGetUpdatedValues()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.PutMappedResult("CommentCountsByBlog", "123", "1", "{'a': 'abc'}", MapReduceIndex.ComputeHash("CommentCountsByBlog", "1"));
			});


			transactionalStorage.Batch(actions =>
			{
				actions.PutMappedResult("CommentCountsByBlog", "123", "1", "{'a': 'def'}", MapReduceIndex.ComputeHash("CommentCountsByBlog", "1"));
			});

			transactionalStorage.Batch(actions =>
			{
				var strings = actions.GetMappedResults("CommentCountsByBlog", "1", MapReduceIndex.ComputeHash("CommentCountsByBlog", "1")).Select(x => x.ToString()).ToArray();
				Assert.Contains("def", strings[0]);
			});
		}

		[Fact]
		public void CanDeleteValueByDocumentId()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.PutMappedResult("CommentCountsByBlog1", "123", "1", "{'a': 'abc'}", MapReduceIndex.ComputeHash("CommentCountsByBlog1", "1"));
				actions.PutMappedResult("CommentCountsByBlog2", "123", "1", "{'a': 'abc'}", MapReduceIndex.ComputeHash("CommentCountsByBlog2", "1"));
			});

			transactionalStorage.Batch(actions =>
			{
				actions.DeleteMappedResultsForDocumentId("123", "CommentCountsByBlog2");
				actions.DeleteMappedResultsForDocumentId("123", "CommentCountsByBlog1");
			});

			transactionalStorage.Batch(actions =>
			{
				Assert.Empty(actions.GetMappedResults("CommentCountsByBlog1", "1", MapReduceIndex.ComputeHash("CommentCountsByBlog1", "1")));
				Assert.Empty(actions.GetMappedResults("CommentCountsByBlog2", "1", MapReduceIndex.ComputeHash("CommentCountsByBlog2", "1")));
			});
		}

		[Fact]
		public void CanDeleteValueByView()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.PutMappedResult("CommentCountsByBlog1", "123", "1", "{'a': 'abc'}", MapReduceIndex.ComputeHash("CommentCountsByBlog1", "1"));
				actions.PutMappedResult("CommentCountsByBlog2", "123", "1", "{'a': 'abc'}", MapReduceIndex.ComputeHash("CommentCountsByBlog2", "1"));
			});


			transactionalStorage.Batch(actions =>
			{
				actions.DeleteMappedResultsForView("CommentCountsByBlog2");
			});

			transactionalStorage.Batch(actions =>
			{
				Assert.NotEmpty(actions.GetMappedResults("CommentCountsByBlog1", "1", MapReduceIndex.ComputeHash("CommentCountsByBlog1", "1")));
				Assert.Empty(actions.GetMappedResults("CommentCountsByBlog2", "1", MapReduceIndex.ComputeHash("CommentCountsByBlog2", "1")));
			});
		}
	}
}