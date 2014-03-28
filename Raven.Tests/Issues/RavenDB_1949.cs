// -----------------------------------------------------------------------
//  <copyright file="RavenDB-1949.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1949 : TransactionalStorageTestBase
	{
		[Fact]
		public void AttemptToReproduceAccessViolationExceptionOnVoron()
		{
			using (var storage = NewTransactionalStorage("voron", runInMemory: true))
			{
				Etag fromEtag = Etag.Empty;

				var writingTask = Task.Factory.StartNew(() =>
				{
					int index = 0;

					for (int k = 0; k < 10; k++)
					{
						storage.Batch(accessor =>
						{
							for (int i = 0; i < 100; i++)
							{
								Etag firstInBatch = null;

								for (int j = 0; j < 128; j++)
								{
									var add = accessor.Documents.InsertDocument("items/" + index, new RavenJObject(), new RavenJObject(), false);

									if (firstInBatch == null)
										firstInBatch = add.Etag;

									index++;
								}

								accessor.General.PulseTransaction();

								fromEtag = EtagUtil.Increment(firstInBatch, -1);
							}
						});
					}

				}, TaskCreationOptions.LongRunning);


				var readingTask = Task.Factory.StartNew(() =>
				{

					while (writingTask.IsCompleted == false)
					{
						storage.Batch(accessor =>
						{
							accessor.Documents.GetDocumentsAfter(fromEtag, 128);
						});

						Thread.Sleep(100);
					}
				}, TaskCreationOptions.LongRunning);

				Task.WaitAll(writingTask, readingTask);

				Assert.Null(writingTask.Exception);
				Assert.Null(readingTask.Exception);
			}
		}
	}
}