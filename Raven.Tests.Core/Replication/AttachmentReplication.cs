// -----------------------------------------------------------------------
//  <copyright file="AttachmentReplication.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Core.Replication
{
	public class AttachmentReplication : RavenCoreTestBase
	{
		[Fact]
		public void Can_replicate_between_two_instances()
		{
			using (var store1 = GetDocumentStore(dbSuffixIdentifier:"1"))
			using (var store2 = GetDocumentStore(dbSuffixIdentifier:"2"))
			{
				
			}

			//TellFirstInstanceToReplicateToSecondInstance();

			//store1.DatabaseCommands.PutAttachment("ayende", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());

			//Attachment attachment = null;
			//for (int i = 0; i < RetriesCount; i++)
			//{
			//	attachment = store2.DatabaseCommands.GetAttachment("ayende");
			//	if (attachment != null)
			//		break;
			//	Thread.Sleep(100);
			//}

			//Assert.NotNull(attachment);
			//Assert.Equal(new byte[] { 1, 2, 3 }, attachment.Data().ReadData());
		}

	}
}