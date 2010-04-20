using System;
using System.Linq;
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
			transactionalStorage = new TransactionalStorage("raven.db.test.esent", () => { });
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
				actions.PutMappedResult("CommentCountsByBlog", "123", "1", "abc");
				actions.PutMappedResult("CommentCountsByBlog", "324", "2", "def");
				actions.PutMappedResult("CommentCountsByBlog", "321", "1", "ijg");
				actions.Commit();
			});
		}

		[Fact]
		public void CanUpdateValue()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.PutMappedResult("CommentCountsByBlog", "123", "1", "abc");
				actions.Commit();
			});


			transactionalStorage.Batch(actions =>
			{
				actions.PutMappedResult("CommentCountsByBlog", "123", "1", "def");
				actions.Commit();
			});
		}

		[Fact]
		public void CanStoreAndGetValues()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.PutMappedResult("CommentCountsByBlog", "123", "1", "abc");
				actions.PutMappedResult("CommentCountsByBlog", "324", "2", "def");
				actions.PutMappedResult("CommentCountsByBlog", "321", "1", "ijg");
				actions.Commit();
			});


			transactionalStorage.Batch(actions =>
			{
				var vals = actions.GetMappedResults("CommentCountsByBlog", "1").ToArray();
				Assert.Equal(2, vals.Length);
				Assert.Contains("abc", vals);
				Assert.Contains("ijg", vals);
				actions.Commit();
			});
		}


		[Fact]
		public void CanUpdateValueAndGetUpdatedValues()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.PutMappedResult("CommentCountsByBlog", "123", "1", "abc");
				actions.Commit();
			});


			transactionalStorage.Batch(actions =>
			{
				actions.PutMappedResult("CommentCountsByBlog", "123", "1", "def");
				actions.Commit();
			});

			transactionalStorage.Batch(actions =>
			{
				Assert.Contains("def", actions.GetMappedResults("CommentCountsByBlog", "1"));
				actions.Commit();
			});
		}

		[Fact]
		public void CanDeleteValueByDocumentId()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.PutMappedResult("CommentCountsByBlog1", "123", "1", "abc");
				actions.PutMappedResult("CommentCountsByBlog2", "123", "1", "abc");
				actions.Commit();
			});

			transactionalStorage.Batch(actions =>
			{
				actions.DeleteMappedResultsForDocumentId("123", "CommentCountsByBlog2");
				actions.DeleteMappedResultsForDocumentId("123", "CommentCountsByBlog1");
				actions.Commit();
			});

			transactionalStorage.Batch(actions =>
			{
				Assert.Empty(actions.GetMappedResults("CommentCountsByBlog1", "1"));
				Assert.Empty(actions.GetMappedResults("CommentCountsByBlog2", "1"));
				actions.Commit();
			});
		}

		[Fact]
		public void CanDeleteValueByView()
		{
			transactionalStorage.Batch(actions =>
			{
				actions.PutMappedResult("CommentCountsByBlog1", "123", "1", "abc");
				actions.PutMappedResult("CommentCountsByBlog2", "123", "1", "abc");
				actions.Commit();
			});


			transactionalStorage.Batch(actions =>
			{
				actions.DeleteMappedResultsForView("CommentCountsByBlog2");
				actions.Commit();
			});

			transactionalStorage.Batch(actions =>
			{
				Assert.NotEmpty(actions.GetMappedResults("CommentCountsByBlog1", "1"));
				Assert.Empty(actions.GetMappedResults("CommentCountsByBlog2", "1"));
				actions.Commit();
			});
		}
	}
}