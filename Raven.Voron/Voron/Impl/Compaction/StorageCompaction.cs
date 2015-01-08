// -----------------------------------------------------------------------
//  <copyright file="Compaction.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using Voron.Impl.Paging;
using Voron.Trees;

namespace Voron.Impl.Compaction
{
	public unsafe static class StorageCompaction
	{
		public static void Execute(StorageEnvironmentOptions srcOptions, StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions compactOptions, long maxTransactionSizeInBytes = 8 * 1024 * 1024)
		{
			long minimalCompactedDataFileSize;

			using(var existingEnv = new StorageEnvironment(srcOptions))
			using (var compactedEnv = new StorageEnvironment(compactOptions))
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
									var transactionDataSize = 0L;

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
														transactionDataSize += multiValue.Size;

													} while (multiTreeIterator.MoveNext());
												}
											}
											else
											{
												using (var value = existingTree.Read(key).Reader.AsStream())
												{
													newTree.Add(key, value);
													transactionDataSize += value.Length;
												}
											}
										} while (transactionDataSize < maxTransactionSizeInBytes && existingTreeIterator.MoveNext());

										txw.Commit();
									}
								} while (existingTreeIterator.MoveNext());
							}
						}

					} while (rootIterator.MoveNext());
				}
				compactedEnv.ForceLogFlushToDataFile(null, allowToFlushOverwrittenPages: true, forceDataFileSync: true);

				minimalCompactedDataFileSize = compactedEnv.NextPageNumber*AbstractPager.PageSize;
			}

			using (var compactedDataFile = new FileStream(Path.Combine(compactOptions.BasePath, Constants.DatabaseFilename), FileMode.Open, FileAccess.ReadWrite))
			{
				compactedDataFile.SetLength(minimalCompactedDataFileSize);
			}
		}
	}
}