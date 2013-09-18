using System.Collections;
using FizzWare.NBuilder.Dates;
using Raven.Abstractions.Exceptions;
using Raven.Database.Data;
using Raven.Database.Storage;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Tests.Bundles.MoreLikeThis;
using Xunit;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Data;
using Xunit.Extensions;

namespace Raven.Tests.Storage.Voron
{
	[Trait("VoronTest","StorageActionsTests")]
	[Trait("VoronTest", "AttachementStorage")]
	public	class AttachmentActionsStorageTests : RavenTest
	{
		private ITransactionalStorage NewVoronStorage()
		{
			return NewTransactionalStorage("voron");
		}

		[Fact]
		public void Storage_Initialized_AttachmentActionsStorage_Is_NotNull()
		{
			using (var storage = NewVoronStorage())
			{
				storage.Batch(viewer => Assert.NotNull(viewer.Attachments));
			}
		}

	    [Fact]
        public void AttachmentStorage_GetAttachmentsAfter()
	    {
            const int ITEM_COUNT = 20;
            const int SKIP_INDEX = ITEM_COUNT / 4;
            using (var storage = NewVoronStorage())
            {                
                var inputData = new Dictionary<Etag,RavenJObject>();
                storage.Batch(
                    mutator =>
                    {
                        int index = 0;
                        for (int itemIndex = 0; itemIndex < ITEM_COUNT; itemIndex++)
                        {
                            var item = RavenJObject.FromObject(new { Name = "Bar_" + itemIndex, Key = "Foo_" + index });
                            var dataStream = new MemoryStream();
                            item.WriteTo(dataStream);
                            // ReSharper disable once RedundantAssignment
                            var newEtag = mutator.Attachments.AddAttachment("Foo_" + (index++), null, dataStream, new RavenJObject());
                            inputData.Add(newEtag,item);
                        }
                    });

                var etagAfterSkip = inputData.Skip(SKIP_INDEX).First().Key;

                IList<AttachmentInformation> fetchedAttachmentInformation = null;

                storage.Batch(viewer => fetchedAttachmentInformation = viewer.Attachments.GetAttachmentsAfter(etagAfterSkip,ITEM_COUNT,4096).ToList());

                Assert.NotNull(fetchedAttachmentInformation);

                //SKIP_INDEX + 1 is because Linq's Skip() fetches items >= than index, and GetAttachmentsAfter() fetches items > than index
                var relevantInputEtags = inputData.Skip(SKIP_INDEX + 1)
                                                  .Select(row => row.Key)
                                                  .ToHashSet();

                //verify that all relevant etags were fetched
                var relevantFetchedEtags = fetchedAttachmentInformation.Select(row => row.Etag);
                Assert.True(relevantInputEtags.SetEquals(relevantFetchedEtags));
            }
        }

        [Theory]
        [InlineData(5, 10, 4096)]
        [InlineData(10, 3, 4096)]
        [InlineData(6, 15, 4096)]
        [InlineData(6, 15, 256)]
        [InlineData(5, 0, 4096)]
        [InlineData(5, 15, 0)]
        [InlineData(0, 15, 4096)]
        public void AttachmentStorage_GetAttachmentsAfter_With_TakeParameter_And_MaxSizeParameter(int skipIndex, int take, int maxSize)
        {
            var itemCount = (skipIndex + take) * 3;
            using (var storage = NewVoronStorage())
            {
                var inputData = new Dictionary<Etag, RavenJObject>();
                storage.Batch(
                    mutator =>
                    {
                        int index = 0;
                        for (int itemIndex = 0; itemIndex < itemCount; itemIndex++)
                        {
                            var item = RavenJObject.FromObject(new { Name = "Bar_" + itemIndex, Key = "Foo_" + index });
                            var dataStream = new MemoryStream();
                            item.WriteTo(dataStream);
                            // ReSharper disable once RedundantAssignment
                            var newEtag = mutator.Attachments.AddAttachment("Foo_" + (index++), null, dataStream, new RavenJObject());
                            inputData.Add(newEtag, item);
                        }
                    });

                var etagAfterSkip = inputData.Skip(skipIndex).First().Key;

                IList<AttachmentInformation> fetchedAttachmentInformation = null;
                storage.Batch(viewer => fetchedAttachmentInformation = viewer.Attachments.GetAttachmentsAfter(etagAfterSkip, take, maxSize).ToList());

                if (maxSize == 0 || take == 0)
                    Assert.Empty(fetchedAttachmentInformation);
                else
                {
                    //skipIndex + 1 is because Linq's Skip() fetches items >= than index, and GetAttachmentsAfter() fetches items > than index
                    var inputDataAfterSkip = inputData.Skip(skipIndex + 1)
                        .Take(take)
                        .ToDictionary(item => item.Key, item => item.Value);

                    var filteredInputData = new Dictionary<Etag, RavenJObject>();
                    //limit resulting data by accumulated size
                    var totalSizeCounter = 0;
                    storage.Batch(viewer =>
                    {
                        foreach (var item in inputDataAfterSkip)
                        {
                            var itemSize = viewer.Attachments.GetAttachment(item.Value.Value<string>("Key")).Size;
                            totalSizeCounter += itemSize;

                            if (totalSizeCounter >= maxSize) break;
                            filteredInputData.Add(item.Key, item.Value);
                        }
                    });

                    //verify that all relevant etags were fetched
                    Assert.Empty(filteredInputData.Keys
                        .Except(fetchedAttachmentInformation.Select(row => row.Etag)));
                    Assert.Empty(fetchedAttachmentInformation.Select(row => row.Etag)
                        .Except(filteredInputData.Keys));

                    for (int itemIndex = skipIndex + 1, fetchedItemIndex = 0;
                        itemIndex < skipIndex + 1 + ((filteredInputData.Count < take) ? filteredInputData.Count : take);
                        itemIndex++, fetchedItemIndex++)
                    {
                        var fetchedItem = fetchedAttachmentInformation[fetchedItemIndex];
                        Assert.Equal(fetchedItem.Key, filteredInputData[fetchedItem.Etag].Value<string>("Key"));
                    }
                }
            }
        }

	    [Fact]
        public void AttachmentStorage_GetAttachmentsAfter_NoAttachments_EmptyResultCollection()
	    {
            using (var storage = NewVoronStorage())
            {
                IEnumerable<AttachmentInformation> attachmentInformationList = null;
                storage.Batch(viewer => attachmentInformationList = viewer.Attachments.GetAttachmentsAfter(Etag.InvalidEtag, 25, 500));

                Assert.NotNull(attachmentInformationList);
                Assert.Empty(attachmentInformationList);
            }
	    }

	    [Fact]
	    public void AttachmentStorage_AttachmentAdded_AttachmentDeletedWithNullEtag()
	    {
	        using (var storage = NewVoronStorage())
	        {
	            using (Stream dataStream = new MemoryStream())
	            {
	                var data = RavenJObject.FromObject(new {Name = "Bar"});
	                data.WriteTo(dataStream);

// ReSharper disable once AccessToDisposedClosure
	                storage.Batch(mutator => mutator.Attachments.AddAttachment("Foo", null, dataStream, new RavenJObject()));
                    
                    //delete attachment should not throw ConcurrencyException with etag parameter equal to null
	                Assert.DoesNotThrow(() =>
	                    storage.Batch(viewer => viewer.Attachments.DeleteAttachment("Foo", null)));

                    Attachment fetchedAttachment = null;
                    storage.Batch(viewer => fetchedAttachment = viewer.Attachments.GetAttachment("Foo"));
                    Assert.Null(fetchedAttachment); //nothing returned --> key not found
                }
	        }
	    }

        [Fact]
        public void AttachmentStorage_AttachmentAdded_AttachmentDeletedWithCorrectEtag()
        {
            using (var storage = NewVoronStorage())
            {
                using (Stream dataStream = new MemoryStream())
                {
                    var data = RavenJObject.FromObject(new { Name = "Bar" });
                    data.WriteTo(dataStream);
                    Etag currentEtag = Etag.InvalidEtag;

                    // ReSharper disable once AccessToDisposedClosure
                    storage.Batch(mutator => currentEtag = mutator.Attachments.AddAttachment("Foo", null, dataStream, new RavenJObject()));

                    //delete attachment should not throw ConcurrencyException with etag parameter equal to existing etag
                    Assert.DoesNotThrow(() =>
                        storage.Batch(viewer => viewer.Attachments.DeleteAttachment("Foo", currentEtag)));

                    Attachment fetchedAttachment = null;
                    storage.Batch(viewer => fetchedAttachment = viewer.Attachments.GetAttachment("Foo"));
                    Assert.Null(fetchedAttachment); //nothing returned --> key not found
                }
            }
        }

        [Fact]
        public void AttachmentStorage_AttachmentAdded_AttachmentDeletedWithNotMatchingEtag_ExceptionThrown()
        {
            using (var storage = NewVoronStorage())
            {
                using (Stream dataStream = new MemoryStream())
                {
                    var data = RavenJObject.FromObject(new { Name = "Bar" });
                    data.WriteTo(dataStream);

                    // ReSharper disable once AccessToDisposedClosure
                    storage.Batch(mutator => mutator.Attachments.AddAttachment("Foo", null, dataStream, new RavenJObject()));

                    //on purpose run delete attachment with etag that will always not match existing
                    Assert.Throws<ConcurrencyException>(() =>
                        storage.Batch(viewer => viewer.Attachments.DeleteAttachment("Foo", Etag.Empty)));
                }
            }
        }

	    [Fact]
		public void AttachmentStorage_AttachmentAdded_AttachmentFeched()
		{
			using (var storage = NewVoronStorage())
			{
				using (Stream dataStream = new MemoryStream())
				{
					var data = RavenJObject.FromObject(new { Name = "Bar" });
					data.WriteTo(dataStream);

					storage.Batch(mutator => mutator.Attachments.AddAttachment("Foo", null, dataStream, new RavenJObject()));

					Attachment fetchedAttachment = null;
					storage.Batch(viewer => fetchedAttachment = viewer.Attachments.GetAttachment("Foo"));

					Assert.NotNull(fetchedAttachment);

					RavenJObject fetchedAttachmentData = null;
					Assert.DoesNotThrow(() => 
						{
							using (var fetchedDataStream = fetchedAttachment.Data())
								fetchedAttachmentData = fetchedDataStream.ToJObject();
						});
					Assert.NotNull(fetchedAttachmentData);

					Assert.Equal(fetchedAttachmentData.Keys, data.Keys);
					Assert.Equal(1, fetchedAttachmentData.Count);
					Assert.Equal(fetchedAttachmentData.Value<string>("Name"), data.Value<string>("Name"));
				}
			}
		}

        [Fact]
        public void AttachmentStorage_MultipleAttachmentAdded_AllAttachmentsFeched()
        {
            const int ITEM_COUNT = 25;
            using (var storage = NewVoronStorage())
            {
                var inputData = new List<RavenJObject>();
                for (int itemIndex = 0; itemIndex < ITEM_COUNT; itemIndex++)
                    inputData.Add(RavenJObject.FromObject(new {Name = "Bar_" + itemIndex}));

                storage.Batch(
                    mutator =>
                    {
                        int index = 0;
                        inputData.ForEach(item =>
                        {
                            var dataStream = new MemoryStream();
                            item.WriteTo(dataStream);
// ReSharper disable once RedundantAssignment
                            mutator.Attachments.AddAttachment("Foo_" + (index++), null, dataStream, new RavenJObject());
                        });
                    });           
            

                var fetchedAttachments = new List<Attachment>();


                storage.Batch(viewer =>
                {
                    for (int itemIndex = ITEM_COUNT - 1; itemIndex >= 0; itemIndex--)
                       fetchedAttachments.Add(viewer.Attachments.GetAttachment("Foo_" + itemIndex));
                });

                Assert.NotEmpty(fetchedAttachments);
                Assert.Equal(inputData.Count,fetchedAttachments.Count);

                for (int itemIndex = 0; itemIndex < ITEM_COUNT; itemIndex++)
                {
                    RavenJObject fetchedAttachmentData = null;
                    Assert.DoesNotThrow(() =>
                    {
                        using(var dataStream = fetchedAttachments.First(row => row.Key == "Foo_" + itemIndex).Data())
                            fetchedAttachmentData = dataStream.ToJObject();
                    });
                    Assert.NotNull(fetchedAttachmentData);

                    Assert.Equal(inputData[itemIndex].Value<string>("Name"), fetchedAttachmentData.Value<string>("Name"));
                }
            }
        }


		[Fact]
		public void AttachmentStorage_Attachment_WithHeader_Added_AttachmentWithHeadersFeched()
		{
			using (var storage = NewVoronStorage())
			{
				using (Stream dataStream = new MemoryStream())
				{
					var data = RavenJObject.FromObject(new { Name = "Bar" });					
					data.WriteTo(dataStream);
					
					var headers = RavenJObject.FromObject(new { Meta = "Data" });
                    
                    // ReSharper disable once AccessToDisposedClosure
                    storage.Batch(mutator => mutator.Attachments
													.AddAttachment("Foo", null, dataStream, headers));

					Attachment fetchedAttachment = null;
					storage.Batch(viewer => fetchedAttachment = viewer.Attachments.GetAttachment("Foo"));

					Assert.NotNull(fetchedAttachment.Metadata);

					Assert.Equal(headers.Keys, fetchedAttachment.Metadata.Keys);
					Assert.Equal(1, fetchedAttachment.Metadata.Count);
					Assert.Equal(headers.Value<string>("Meta"), fetchedAttachment.Metadata.Value<string>("Meta"));
					
				}
			}
		}


	    [Theory]
        [InlineData(25, 0, 20)]
        [InlineData(25, 3, 5)]
        [InlineData(14, 5, 3)]
        [InlineData(10, 0, 0)]
        public void AttachmentStorage_GetAttachmentsStartingWith(int itemCount, int start, int pageSize)
	    {
	        using (var storage = NewVoronStorage())
	        {
	            var inputData = new Dictionary<string, RavenJObject>();
	            storage.Batch(mutator =>
	            {
	                for (int itemIndex = 0; itemIndex < itemCount; itemIndex++)
	                {
	                    var keyPrefix = (itemIndex%2 == 0) ? "Foo" : "Bar";
	                    var data = RavenJObject.FromObject(new {Name = "Bar" + itemIndex});
	                    var dataStream = new MemoryStream();
	                    data.WriteTo(dataStream);

	                    // ReSharper disable once AccessToDisposedClosure
	                    mutator.Attachments.AddAttachment(keyPrefix + itemIndex, null, dataStream, new RavenJObject());
	                    inputData.Add(keyPrefix + itemIndex, data);

	                }
	            });

	            IList<AttachmentInformation> attachmentInformationList = null;
                storage.Batch(viewer => attachmentInformationList = viewer.Attachments.GetAttachmentsStartingWith("Foo",start,pageSize).ToList());

                Assert.NotNull(attachmentInformationList);
	            var relevantInputKeys = new HashSet<string>(inputData.Where(kvp => kvp.Key.StartsWith("Foo"))
                                                                                          .OrderBy(kvp => kvp.Key)
	                                                                                      .Select(row => row.Key.ToLowerInvariant())
	                                                                                      .Skip(start)
	                                                                                      .Take(pageSize));

	            var fetchedKeys = new HashSet<string>(attachmentInformationList.Select(row => row.Key.ToLowerInvariant()));
                
                Assert.True(fetchedKeys.Count <= pageSize);
                Assert.True(relevantInputKeys.SetEquals(fetchedKeys));
            }
	    }

	    [Theory]
        [InlineData(10, 0)]
        [InlineData(10, 5)]
        [InlineData(5, 6)]
        [InlineData(0, 1)]
        public void AttachmentStorage_GetAttachmentsByReverseUpdateOrder(int itemCount, int start)
	    {
	        using (var storage = NewVoronStorage())
	        {
	            var inputData = new Dictionary<Etag, RavenJObject>();
	            storage.Batch(mutator =>
	            {
	                for (int itemIndex = 0; itemIndex < itemCount; itemIndex++)
	                {
	                    var keyPrefix = (itemIndex%2 == 0) ? "Foo" : "Bar";
	                    var data = RavenJObject.FromObject(new {Name = "Bar" + itemIndex});
	                    var dataStream = new MemoryStream();
	                    data.WriteTo(dataStream);

	                    // ReSharper disable once AccessToDisposedClosure
	                    var itemEtag = mutator.Attachments.AddAttachment(keyPrefix + itemIndex, null, dataStream,
	                        new RavenJObject());
	                    inputData.Add(itemEtag, data);

	                }
	            });

	            IList<AttachmentInformation> attachmentInformationList = null;
	            storage.Batch(
	                viewer =>
	                    attachmentInformationList = viewer.Attachments.GetAttachmentsByReverseUpdateOrder(start).ToList());

	            if (start >= itemCount)
	                Assert.Empty(attachmentInformationList);
	            else
	            {
	                var relevantInputEtags = inputData.OrderByDescending(kvp => kvp.Key)
	                    .Skip(start)
	                    .Select(kvp => kvp.Key)
	                    .ToList();

	                var fetchedEtags = attachmentInformationList.Select(row => row.Etag)
	                    .ToList();

	                Assert.Equal(relevantInputEtags, fetchedEtags);
	            }
	        }
	    }
	}
}
