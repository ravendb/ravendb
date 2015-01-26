// -----------------------------------------------------------------------
//  <copyright file="AddingStructValues.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Runtime.InteropServices;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Trees
{
	public class RavenDB_3114_WorkingWithStructs : StorageTest
	{
		[StructLayout(LayoutKind.Explicit, Pack = 1)]
		public struct Stats
		{
			[FieldOffset(0)]
			public int Attempts;

			[FieldOffset(4)]
			public int Successes;

			[FieldOffset(8)]
			public long Erros;

			[FieldOffset(16)]
			[MarshalAs(UnmanagedType.U1)]
			public bool IsValid;

			[FieldOffset(17)]
			public long IndexedAtTicks;
		}

		[StructLayout(LayoutKind.Sequential, Pack = 1, CharSet = CharSet.Unicode)]
		public struct MappedResultsStats
		{
			public int View;
			public string ReduceKey;
			public string DocId;
			[MarshalAs(UnmanagedType.ByValArray, SizeConst = 16)]
			public byte[] Etag;
			public int Bucket;
			public long TimestampTicks;
		}

		[StructLayout(LayoutKind.Explicit, Pack = 1)]
		public struct Operation
		{
			[FieldOffset(0)]
			public int Id;

			[FieldOffset(4)]
			public Stats Stats;
		}

		public struct StructWithoutExplicitLayout
		{
			public int Field;
		}

		[StructLayout(LayoutKind.Explicit)]
		public struct StructWithExplicitLayoutButWithoutPack
		{
			[FieldOffset(0)]
			public int Field;
		}

		[Fact]
		public void ShouldThrowIfStructDoesntHaveExplicitLayout()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Assert.Throws<InvalidDataException>(() => tx.State.Root.Write("item", new StructWithoutExplicitLayout()));
				Assert.Throws<InvalidDataException>(() => tx.State.Root.Read<StructWithoutExplicitLayout>("item"));

				Assert.Throws<InvalidDataException>(() => tx.State.Root.Write("item", new StructWithExplicitLayoutButWithoutPack()));
				Assert.Throws<InvalidDataException>(() => tx.State.Root.Read<StructWithExplicitLayoutButWithoutPack>("item"));
			}
		}

		[Fact]
		public void CanReadAndWriteStructsFromTrees()
		{
			var indexedAt = new DateTime(2015, 1, 20);

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "stats");

				tree.Write("stats/1", new Stats()
				{
					Attempts = 5,
					Erros = -1,
					Successes = 4,
					IsValid = true,
					IndexedAtTicks = indexedAt.Ticks
				});

				tree.Write("operations/1", new Operation()
				{
					Id = 1,
					Stats = new Stats
					{
						Attempts = 10,
						Successes = 10,
						IsValid = false,
						IndexedAtTicks = indexedAt.Ticks
					}
				});

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var tree = tx.ReadTree("stats");

				var stats = tree.Read<Stats>("stats/1").Value;

				Assert.Equal(5, stats.Attempts);
				Assert.Equal(-1, stats.Erros);
				Assert.Equal(4, stats.Successes);
				Assert.True(stats.IsValid);
				Assert.Equal(indexedAt, new DateTime(stats.IndexedAtTicks));

				var operation = tree.Read<Operation>("operations/1").Value;

				Assert.Equal(1, operation.Id);
				Assert.Equal(10, operation.Stats.Successes);
				Assert.Equal(10, operation.Stats.Attempts);
				Assert.False(operation.Stats.IsValid);
				Assert.Equal(indexedAt, new DateTime(operation.Stats.IndexedAtTicks));
			}
		}

		[Fact]
		public void CanDeleteStructsFromTrees()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "stats");

				tree.Write("stats/1", new Stats()
				{
					Attempts = 5,
					Erros = -1,
					Successes = 4
				});

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = tx.ReadTree("stats");

				tree.Delete("stats/1");

				var stats = tree.Read<Stats>("stats/1");

				Assert.Null(stats);
			}
		}

		[Fact]
		public void CanWriteStructsByUsingWriteBatch()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "stats");
				Env.CreateTree(tx, "operations");

				tx.Commit();
			}
			var batch = new WriteBatch();

			batch.AddStruct("stats/1", new Stats()
			{
				Attempts = 5,
				Erros = -1,
				Successes = 4
			}, "stats");

			batch.AddStruct("operations/1", new Operation()
			{
				Id = 1,
				Stats = new Stats
				{
					Attempts = 10,
					Successes = 10
				}
			}, "operations");

			using (var snapshot = Env.CreateSnapshot())
			{
				var stats = snapshot.ReadStruct<Stats>("stats", "stats/1", batch).Value;

				Assert.Equal(5, stats.Attempts);
				Assert.Equal(-1, stats.Erros);
				Assert.Equal(4, stats.Successes);
			}

			Env.Writer.Write(batch);

			using (var snapshot = Env.CreateSnapshot())
			{
				var operation = snapshot.ReadStruct<Operation>("operations", "operations/1").Value;

				Assert.Equal(1, operation.Id);
				Assert.Equal(10, operation.Stats.Successes);
				Assert.Equal(10, operation.Stats.Attempts);
			}

			batch.Delete("stats/1", "stats");

			using (var snapshot = Env.CreateSnapshot())
			{
				var stats = snapshot.ReadStruct<Stats>("stats", "stats/1", batch);

				Assert.Null(stats);
			}
		}

		[Fact]
		public void CanReadStructsFromTreeIterator()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "stats");

				tree.Write("items/1", new Stats()
				{
					Attempts = 1
				});

				tree.Write("items/2", new Stats()
				{
					Attempts = 2
				});


				tree.Write("items/3", new Stats()
				{
					Attempts = 3
				});

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var iterator = tx.ReadTree("stats").Iterate();

				iterator.Seek(Slice.BeforeAllKeys);

				var count = 0;

				do
				{
					var stats = iterator.ReadStructForCurrent<Stats>();

					count++;

					Assert.Equal(count, stats.Attempts);

				} while (iterator.MoveNext());

				Assert.Equal(3, count);
			}
		}

		[Fact]
		public void StructuresCanHaveStrings()
		{
			var now = DateTime.Now;
			var etag = new byte[16]
			{
				1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6
			};

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "stats");

				tree.Write("items/1", new MappedResultsStats
				{
					View = 3,
					ReduceKey = "reduce_key",
					Bucket = 1024,
					DocId = "orders/1",
					Etag = etag,
					TimestampTicks = now.Ticks
				});

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var readTree = tx.ReadTree("stats");

				var mappedResults = readTree.Read<MappedResultsStats>("items/1").Value;

				Assert.Equal(3, mappedResults.View);
				Assert.Equal("reduce_key", mappedResults.ReduceKey);
				Assert.Equal(1024, mappedResults.Bucket);
				Assert.Equal("orders/1", mappedResults.DocId);
				Assert.Equal(etag, mappedResults.Etag);
				Assert.Equal(now, new DateTime(mappedResults.TimestampTicks));
			}
		}
	}
}