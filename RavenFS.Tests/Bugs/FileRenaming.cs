using System;
using System.Collections.Specialized;
using System.Linq;
using Xunit;

namespace RavenFS.Tests.Bugs
{
    public class FileRenaming : StorageTest
    {
		private readonly NameValueCollection metadataWithEtag = new NameValueCollection()
		                                               	{
		                                               		{"ETag", "\"" + Guid.Empty +"\""}
		                                               	};

        [Fact]
        public void Should_rename_file_and_content()
        {
            transactionalStorage.Batch(
                accessor =>
                {
					accessor.PutFile("test.bin", 3, metadataWithEtag);
                    var pageId = accessor.InsertPage(new byte[] { 1, 2, 3 }, 3);
                    accessor.AssociatePage("test.bin", pageId, 0, 3);
                    accessor.CompleteFileUpload("test.bin");
                });

            transactionalStorage.Batch(
                accessor => accessor.RenameFile("test.bin", "test.result.bin"));

            transactionalStorage.Batch(
                accessor =>
                {
                    var pages = accessor.GetFile("test.result.bin", 0, 1);
                    var buffer = new byte[3];
                    accessor.ReadPage(pages.Pages.First().Id, buffer);
                    Assert.Equal(1, buffer[0]);
                });

        }

        [Fact]
        public void Should_rename_file_and_content_after_deleting_some_other_file()
        {
            // create 1st file
            transactionalStorage.Batch(
                accessor =>
                    {
						accessor.PutFile("test0.bin", 3, metadataWithEtag);
                        var pageId = accessor.InsertPage(new byte[] { 1, 2, 3 }, 3);
                        accessor.AssociatePage("test0.bin", pageId, 0, 3);
                        accessor.CompleteFileUpload("test0.bin");
                    });
            // create 2nd file
            transactionalStorage.Batch(
                accessor =>
                    {
						accessor.PutFile("test1.bin", 3, metadataWithEtag);
                        var pageId = accessor.InsertPage(new byte[] { 4, 5, 6 }, 3);
                        accessor.AssociatePage("test1.bin", pageId, 0, 3);
                        accessor.CompleteFileUpload("test1.bin");
                    });
            // remove 1st file
            transactionalStorage.Batch(
                accessor =>
                    {
                        // WARN Test passes if you comment out the following line.
                       accessor.Delete("test0.bin");
                    });
            // rename the 2nd file
            transactionalStorage.Batch(
                accessor => accessor.RenameFile("test1.bin", "test.result.bin"));
            // check the renamed file
            transactionalStorage.Batch(
                accessor =>
                {
                    var pages = accessor.GetFile("test.result.bin", 0, 1);
                    var buffer = new byte[3];
                    accessor.ReadPage(pages.Pages.First().Id, buffer);
                    Assert.Equal(4, buffer[0]);
                });
        }
    }
}
