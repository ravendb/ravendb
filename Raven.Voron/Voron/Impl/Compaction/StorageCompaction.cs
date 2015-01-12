// -----------------------------------------------------------------------
//  <copyright file="Compaction.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using Voron.Impl.Paging;
using Voron.Trees;

namespace Voron.Impl.Compaction
{
	public unsafe static class StorageCompaction
	{
		public const string CannotCompactBecauseOfIncrementalBackup = "Cannot compact a storage that supports incremental backups. The compact operation changes internal data structures on which the incremental backup relays.";

		public static void Execute(StorageEnvironmentOptions srcOptions, StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions compactOptions)
		{
			if (srcOptions.IncrementalBackupEnabled)
				throw new InvalidOperationException(CannotCompactBecauseOfIncrementalBackup);

			long minimalCompactedDataFileSize;

			srcOptions.ManualFlushing = true; // prevent from flushing during compaction - we shouldn't touch any source files
			compactOptions.ManualFlushing = true; // let us flush manually during data copy

			using(var existingEnv = new StorageEnvironment(srcOptions))
			using (var compactedEnv = new StorageEnvironment(compactOptions))
			{
				CopyTrees(existingEnv, compactedEnv);

				compactedEnv.FlushLogToDataFile(allowToFlushOverwrittenPages: true);
				compactedEnv.Journal.Applicator.SyncDataFile(compactedEnv.OldestTransaction);
				compactedEnv.Journal.Applicator.DeleteCurrentAlreadyFlushedJournal();

				minimalCompactedDataFileSize = compactedEnv.NextPageNumber*AbstractPager.PageSize;
			}

			using (var compactedDataFile = new FileStream(Path.Combine(compactOptions.BasePath, Constants.DatabaseFilename), FileMode.Open, FileAccess.ReadWrite))
			{
				compactedDataFile.SetLength(minimalCompactedDataFileSize);
			}
		}

		private static void CopyTrees(StorageEnvironment existingEnv, StorageEnvironment compactedEnv)
		{
			using (var rootIterator = existingEnv.State.Root.Iterate())
			{
				if (rootIterator.Seek(Slice.BeforeAllKeys) == false)
					return;

				do
				{
					var treeName = rootIterator.CurrentKey.ToString();

					using (var txr = existingEnv.NewTransaction(TransactionFlags.Read))
					{
						var existingTree = existingEnv.State.GetTree(txr, treeName);

						using (var existingTreeIterator = existingTree.Iterate())
						{
							if (existingTreeIterator.Seek(Slice.BeforeAllKeys) == false)
								continue;

							using (var txw = compactedEnv.NewTransaction(TransactionFlags.ReadWrite))
							{
								compactedEnv.CreateTree(txw, treeName);
								txw.Commit();
							}

							do
							{
								var transactionSize = 0L;

								using (var txw = compactedEnv.NewTransaction(TransactionFlags.ReadWrite))
								{
									var newTree = txw.ReadTree(treeName);

									do
									{
										var key = existingTreeIterator.CurrentKey;

										if (existingTreeIterator.Current->Flags == NodeFlags.MultiValuePageRef)
										{
											using (var multiTreeIterator = existingTree.MultiRead(key))
											{
												if (multiTreeIterator.Seek(Slice.BeforeAllKeys) == false)
													continue;

												do
												{
													var multiValue = multiTreeIterator.CurrentKey;
													newTree.MultiAdd(key, multiValue);
													transactionSize += multiValue.Size;
												} while (multiTreeIterator.MoveNext());
											}
										}
										else
										{
											using (var value = existingTree.Read(key).Reader.AsStream())
											{
												newTree.Add(key, value);
												transactionSize += value.Length;
											}
										}
									} while (transactionSize < compactedEnv.Options.MaxLogFileSize/2 && existingTreeIterator.MoveNext());

									txw.Commit();
								}

								compactedEnv.FlushLogToDataFile();

							} while (existingTreeIterator.MoveNext());
						}
					}
				} while (rootIterator.MoveNext());
			}
		}
	}
}