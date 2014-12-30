// -----------------------------------------------------------------------
//  <copyright file="Compaction.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Voron.Trees;

namespace Voron.Impl.Compaction
{
	public unsafe static class StorageCompaction
	{
		public static void Execute(string srcPath, string compactPath)
		{
			using(var existingEnv = new StorageEnvironment(StorageEnvironmentOptions.ForPath(srcPath))) // TODO arek - temp path, journal's path
			using (var compactedEnv = new StorageEnvironment(StorageEnvironmentOptions.ForPath(compactPath)))
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

							using (var treeIterator = existingTree.Iterate())
							{
								if (treeIterator.Seek(Slice.BeforeAllKeys) == false)
									continue;

								using (var txw = compactedEnv.NewTransaction(TransactionFlags.ReadWrite))
								{
									var newTree = compactedEnv.CreateTree(txw, treeName);

									do
									{
										var key = treeIterator.CurrentKey;

										if (treeIterator.Current->Flags == NodeFlags.MultiValuePageRef)
										{
											using (var multiTreeIterator = existingTree.MultiRead(key))
											{
												if (multiTreeIterator.Seek(Slice.BeforeAllKeys) == false)
													continue;

												do
												{
													var multiValue = multiTreeIterator.CurrentKey;

													newTree.MultiAdd(key, multiValue);

												} while (multiTreeIterator.MoveNext());
											}
										}
										else
										{
											using (var value = existingTree.Read(key).Reader.AsStream())
											{
												newTree.Add(key, value);
											}
										}
									} while (treeIterator.MoveNext());

									txw.Commit(); // TODO do not commit the entire tree in a single transaction
								}
							}
						}

					} while (rootIterator.MoveNext());
				}
				compactedEnv.ForceLogFlushToDataFile(null, true);
			}


		}
	}
}