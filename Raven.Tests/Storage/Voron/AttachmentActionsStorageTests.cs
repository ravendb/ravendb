namespace Raven.Tests.Storage.Voron
{
	using Raven.Abstractions.Exceptions;
	using Raven.Database.Data;
	using Raven.Json.Linq;

	using System.Collections.Generic;
	using System.IO;
	using System.Linq;

	using Raven.Abstractions.Data;

	using Xunit;

	using Raven.Abstractions.Extensions;

	using Xunit.Extensions;

	[Trait("VoronTest", "StorageActionsTests")]
	[Trait("VoronTest", "AttachementStorage")]
	public class AttachmentActionsStorageTests : TransactionalStorageTestBase
	{
		[Theory]
		[PropertyData("Storages")]
		public void Storage_Initialized_AttachmentActionsStorage_Is_NotNull(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(viewer => Assert.NotNull(viewer.Attachments));
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void AttachmentStorage_GetAttachmentsAfter(string requestedStorage)
		{
			const int ITEM_COUNT = 20;
			const int SKIP_INDEX = ITEM_COUNT / 4;
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				var inputData = new Dictionary<Etag, RavenJObject>();
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
							inputData.Add(newEtag, item);
						}
					});

				var etagAfterSkip = inputData.Skip(SKIP_INDEX).First().Key;

				IList<AttachmentInformation> fetchedAttachmentInformation = null;

				storage.Batch(viewer => fetchedAttachmentInformation = viewer.Attachments.GetAttachmentsAfter(etagAfterSkip, ITEM_COUNT, 4096).ToList());

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
		[PropertyData("Storages")]
		public void AttachmentStorage_GetAttachmentsAfter_With_TakeParameter_And_MaxSizeParameter(string requestedStorage)
		{
			var input = new List<dynamic>
			           {
				           new { SkipIndex = 5, Take = 10, MaxSize = 4096},
						   new { SkipIndex = 10, Take = 3, MaxSize = 4096},
						   new { SkipIndex = 6, Take = 15, MaxSize = 4096},
						   new { SkipIndex = 6, Take = 15, MaxSize = 256},
						   new { SkipIndex = 5, Take = 0, MaxSize = 4096},
						   new { SkipIndex = 5, Take = 15, MaxSize = 0},
						   new { SkipIndex = 0, Take = 15, MaxSize = 4096},
					   };

			foreach (var d in input)
			{
				var skipIndex = (int)d.SkipIndex;
				var take = (int)d.Take;
				var maxSize = (int)d.MaxSize;

				var itemCount = (skipIndex + take) * 3;
				using (var storage = NewTransactionalStorage(requestedStorage))
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
		}

		[Theory]
		[PropertyData("Storages")]
		public void AttachmentStorage_GetAttachmentsAfter_NoAttachments_EmptyResultCollection(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				IEnumerable<AttachmentInformation> attachmentInformationList = null;
				storage.Batch(viewer => attachmentInformationList = viewer.Attachments.GetAttachmentsAfter(Etag.InvalidEtag, 25, 500));

				Assert.NotNull(attachmentInformationList);
				Assert.Empty(attachmentInformationList);
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void AttachmentStorage_AttachmentAdded_AttachmentDeletedWithNullEtag(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				using (Stream dataStream = new MemoryStream())
				{
					var data = RavenJObject.FromObject(new { Name = "Bar" });
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

		[Theory]
		[PropertyData("Storages")]
		public void AttachmentStorage_AttachmentAdded_AttachmentDeletedWithCorrectEtag(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
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

		[Theory]
		[PropertyData("Storages")]
		public void AttachmentStorage_AttachmentAdded_AttachmentDeletedWithNotMatchingEtag_ExceptionThrown(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
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

		[Theory]
		[PropertyData("Storages")]
		public void AttachmentStorage_AttachmentAdded_AttachmentFeched(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				using (Stream dataStream = new MemoryStream())
				{
					var data = RavenJObject.FromObject(new { Name = "Bar" });
					data.WriteTo(dataStream);
					dataStream.Position = 0;
					storage.Batch(mutator => mutator.Attachments.AddAttachment("Foo", null, dataStream, new RavenJObject()));

					Attachment fetchedAttachment = null;
					storage.Batch(viewer => fetchedAttachment = viewer.Attachments.GetAttachment("Foo"));

					Assert.NotNull(fetchedAttachment);

					RavenJObject fetchedAttachmentData = null;
					Assert.DoesNotThrow(() =>
						storage.Batch(viewer =>
						{
							using (var fetchedDataStream = fetchedAttachment.Data())
								fetchedAttachmentData = fetchedDataStream.ToJObject();
						}));
					Assert.NotNull(fetchedAttachmentData);

					Assert.Equal(fetchedAttachmentData.Keys, data.Keys);
					Assert.Equal(1, fetchedAttachmentData.Count);
					Assert.Equal(fetchedAttachmentData.Value<string>("Name"), data.Value<string>("Name"));
				}
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void AttachmentStorage_MultipleAttachmentAdded_AllAttachmentsFeched(string requestedStorage)
		{
			const int ITEM_COUNT = 25;
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				var inputData = new List<RavenJObject>();
				for (int itemIndex = 0; itemIndex < ITEM_COUNT; itemIndex++)
					inputData.Add(RavenJObject.FromObject(new { Name = "Bar_" + itemIndex }));

				storage.Batch(
					mutator =>
					{
						int index = 0;
						inputData.ForEach(item =>
						{
							var dataStream = new MemoryStream();
							item.WriteTo(dataStream);
							dataStream.Position = 0; 
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
				Assert.Equal(inputData.Count, fetchedAttachments.Count);

				for (int itemIndex = 0; itemIndex < ITEM_COUNT; itemIndex++)
				{
					RavenJObject fetchedAttachmentData = null;
					Assert.DoesNotThrow(() => 
						storage.Batch(viewer =>
						{
							using (var dataStream = fetchedAttachments.First(row => row.Key == "Foo_" + itemIndex).Data())
								fetchedAttachmentData = dataStream.ToJObject();
						}));
					Assert.NotNull(fetchedAttachmentData);

					Assert.Equal(inputData[itemIndex].Value<string>("Name"), fetchedAttachmentData.Value<string>("Name"));
				}
			}
		}


		[Theory]
		[PropertyData("Storages")]
		public void AttachmentStorage_Attachment_WithHeader_Added_AttachmentWithHeadersFeched(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
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
		[PropertyData("Storages")]
		public void AttachmentStorage_GetAttachmentsStartingWith(string requestedStorage)
		{
			var input = new List<dynamic>
			           {
				           new { ItemCount = 25, Start = 0, PageSize = 20 },
						   new { ItemCount = 25, Start = 3, PageSize = 5 },
						   new { ItemCount = 14, Start = 5, PageSize = 3 },
						   new { ItemCount = 10, Start = 0, PageSize = 0 },
			           };

			foreach (var d in input)
			{
				var itemCount = (int)d.ItemCount;
				var start = (int)d.Start;
				var pageSize = (int)d.PageSize;

				using (var storage = NewTransactionalStorage(requestedStorage))
				{
					var inputData = new Dictionary<string, RavenJObject>();
					storage.Batch(mutator =>
					{
						for (int itemIndex = 0; itemIndex < itemCount; itemIndex++)
						{
							var keyPrefix = (itemIndex % 2 == 0) ? "Foo" : "Bar";
							var data = RavenJObject.FromObject(new { Name = "Bar" + itemIndex });
							var dataStream = new MemoryStream();
							data.WriteTo(dataStream);

							// ReSharper disable once AccessToDisposedClosure
							mutator.Attachments.AddAttachment(keyPrefix + itemIndex, null, dataStream, new RavenJObject());
							inputData.Add(keyPrefix + itemIndex, data);

						}
					});

					IList<AttachmentInformation> attachmentInformationList = null;
					storage.Batch(viewer => attachmentInformationList = viewer.Attachments.GetAttachmentsStartingWith("Foo", start, pageSize).ToList());

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
		}

		[Theory]
		[PropertyData("Storages")]
		public void AttachmentStorage_GetAttachmentsByReverseUpdateOrder(string requestedStorage)
		{
			var input = new List<dynamic>
			           {
				           new { ItemCount = 10, Start = 0 },
						   new { ItemCount = 10, Start = 5 },
						   new { ItemCount = 5, Start = 6 },
						   new { ItemCount = 0, Start = 1 },
			           };

			foreach (var d in input)
			{
				var itemCount = (int)d.ItemCount;
				var start = (int)d.Start;

				using (var storage = NewTransactionalStorage(requestedStorage))
				{
					var inputData = new Dictionary<Etag, RavenJObject>();
					storage.Batch(
						mutator =>
						{
							for (int itemIndex = 0; itemIndex < itemCount; itemIndex++)
							{
								var keyPrefix = (itemIndex % 2 == 0) ? "Foo" : "Bar";
								var data = RavenJObject.FromObject(new { Name = "Bar" + itemIndex });
								var dataStream = new MemoryStream();
								data.WriteTo(dataStream);

								// ReSharper disable once AccessToDisposedClosure
								var itemEtag = mutator.Attachments.AddAttachment(keyPrefix + itemIndex, null, dataStream, new RavenJObject());
								inputData.Add(itemEtag, data);

							}
						});

					IList<AttachmentInformation> attachmentInformationList = null;
					storage.Batch(
						viewer => attachmentInformationList = viewer.Attachments.GetAttachmentsByReverseUpdateOrder(start).ToList());

					if (start >= itemCount) Assert.Empty(attachmentInformationList);
					else
					{
						var relevantInputEtags = inputData.OrderByDescending(kvp => kvp.Key).Skip(start).Select(kvp => kvp.Key).ToList();

						var fetchedEtags = attachmentInformationList.Select(row => row.Etag).ToList();

						Assert.Equal(relevantInputEtags, fetchedEtags);
					}
				}
			}
		}
	}
}
