// -----------------------------------------------------------------------
//  <copyright file="RavenDB-1949.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
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
				var writingTask = Task.Factory.StartNew(() =>
				{
					int index = 0;

					for (int k = 0; k < 10; k++)
					{
						storage.Batch(accessor =>
						{
							for (int i = 0; i < 100; i++)
							{
								for (int j = 0; j < 128; j++)
								{
									accessor.Documents.InsertDocument("items/" + index, new RavenJObject(), new RavenJObject(), false);

									index++;
								}

								accessor.General.PulseTransaction();
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
							accessor.Documents.GetDocumentsAfter(Etag.Empty, 128*1024);
							Thread.Sleep(100);
						});
					}
				}, TaskCreationOptions.LongRunning);

				Task.WaitAll(writingTask, readingTask);

				Assert.Null(writingTask.Exception);
				Assert.Null(readingTask.Exception);
			}
		}
	}
}