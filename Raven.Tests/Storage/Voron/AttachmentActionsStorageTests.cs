using Raven.Database.Storage;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Data;

namespace Raven.Tests.Storage.Voron
{
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
		public void AttachmentStorage_AttachmentAdded_AttachmentFeched()
		{
			//it is not a passing test yet --> work in progress
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
							{
								fetchedAttachmentData = fetchedDataStream.ToJObject();
							}
						});
					Assert.NotNull(fetchedAttachmentData);

					Assert.Equal(fetchedAttachmentData.Keys, data.Keys);
					Assert.Equal(1, fetchedAttachmentData.Count);
					Assert.Equal(fetchedAttachmentData.Value<string>("Name"), data.Value<string>("Name"));
				}
			}
		}

		[Fact]
		public void AttachmentStorage_Attachment_WithHeader_Added_AttachmentWithHeadersFeched()
		{
			//it is not a passing test yet --> work in progress
			using (var storage = NewVoronStorage())
			{
				using (Stream dataStream = new MemoryStream())
				{
					var data = RavenJObject.FromObject(new { Name = "Bar" });					
					data.WriteTo(dataStream);
					
					var headers = RavenJObject.FromObject(new { Meta = "Data" });
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
	
	}
}
