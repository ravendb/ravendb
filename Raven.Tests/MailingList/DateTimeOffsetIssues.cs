using System;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class DateTimeOffsetIssues : RavenTest
	{
		public class Book
		{
			public string Id { get; set; }
			public string Author { get; set; }
		}

		[Fact]
		public void WhenAddingDateTimeToTheMetadaDataThenItIsAddedAsDateTimeOffset()
		{
			using(var _documentStore = NewRemoteDocumentStore())
			{
				// Arrange
				string bookId;
				var expectedDateTime =   DateTime.Parse("2011-11-05T13:09:17.540277"); //this does not work!
				//var expectedDateTime = DateTime.Parse("2011-11-05T13:09:17.5402774"); //this works!

				using (var session = _documentStore.OpenSession())
				{
					var entity = new Book { Author = "dane" };
					session.Store(entity);
					session.SaveChanges();
					bookId = entity.Id;
				}

				// Act
				// Add metadata to the entity
				using (var session = _documentStore.OpenSession())
				{
					var book = session.Load<Book>(bookId);
					var metadata = session.Advanced.GetMetadataFor(book);
					metadata.Add("DateTime-ToCheck", RavenJToken.FromObject(expectedDateTime));
					session.SaveChanges();
				}

				// Try get metadata
				using (var session = _documentStore.OpenSession())
				{
					var entity = session.Load<Book>(bookId);
					var metadata = session.Advanced.GetMetadataFor(entity);
					var result = metadata.Value<DateTime>("DateTime-ToCheck"); // No exception is thrown here
					Assert.IsType<DateTime>(result);
					Assert.Equal(expectedDateTime,result);
				}

				// Change the entity
				using (var session = _documentStore.OpenSession())
				{
					var book = session.Load<Book>(bookId);
					book.Author = "Jane Doe";
					session.SaveChanges();
				}

				// Assert
				// Try to get the metadata back as DateTime
				using (var session = _documentStore.OpenSession())
				{
					var entity = session.Load<Book>(bookId);
					var metadata = session.Advanced.GetMetadataFor(entity);
					var result = metadata.Value<DateTime>("DateTime-ToCheck"); // An exception is thrown here, after changing the entity
					Assert.IsType<DateTime>(result);
					Assert.Equal(expectedDateTime, result);
				}
			}
		}

	}
}