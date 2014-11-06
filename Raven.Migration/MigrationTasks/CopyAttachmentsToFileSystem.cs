// -----------------------------------------------------------------------
//  <copyright file="CopyAttachmentsToFileSystem.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;

namespace Raven.Migration.MigrationTasks
{
	public class CopyAttachmentsToFileSystem : MigrationTask
	{
		private readonly RavenConnectionStringOptions databaseConnectionOptions;
		private readonly RavenConnectionStringOptions fileSystemConnectionOptions;
		private readonly string fileSystemName;
		private readonly bool deleteCopiedAttachments;
		private readonly int batchSize;

		public CopyAttachmentsToFileSystem(RavenConnectionStringOptions databaseConnectionOptions, RavenConnectionStringOptions fileSystemConnectionOptions, string fileSystemName, bool deleteCopiedAttachments, int batchSize)
		{
			this.databaseConnectionOptions = databaseConnectionOptions;
			this.fileSystemConnectionOptions = fileSystemConnectionOptions;
			this.fileSystemName = fileSystemName;
			this.deleteCopiedAttachments = deleteCopiedAttachments;
			this.batchSize = batchSize;
		}

		public override void Execute()
		{
			using (var store = CreateStore(databaseConnectionOptions))
			using (var fsclient = CreateFileSystemClient(fileSystemConnectionOptions ?? databaseConnectionOptions, fileSystemName))
			{
				var commands = store.DatabaseCommands;

				var totalAttachmentCount = commands.GetStatistics().CountOfAttachments;
				var startEtag = Etag.Empty;
				var copiedAttachments = 0;


				while (true)
				{
					var batchInfo = commands.GetAttachments(0, startEtag, batchSize);

					if(batchInfo == null || batchInfo.Length == 0)
						break;

					foreach (var attachmentInfo in batchInfo)
					{
						var attachment = commands.GetAttachment(attachmentInfo.Key);

                        fsclient.UploadAsync(attachment.Key, attachment.Data(), attachment.Metadata).Wait();

						startEtag = attachment.Etag;

						if(deleteCopiedAttachments)
							commands.DeleteAttachment(attachment.Key, null);
					}
					copiedAttachments += batchInfo.Length;

					Console.WriteLine("Copied {0} attachments ({1:####} %)", copiedAttachments, copiedAttachments * 100f / totalAttachmentCount);
				}

				Console.WriteLine("Migration task copied {0} attachments to the {1} file system", copiedAttachments, fileSystemName);
			}
		}
	}
}