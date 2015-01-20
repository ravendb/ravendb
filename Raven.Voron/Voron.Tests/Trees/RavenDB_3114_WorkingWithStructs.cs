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
		[StructLayout(LayoutKind.Explicit)]
		public struct Stats
		{
			[FieldOffset(0)]
			public int Attempts;

			[FieldOffset(4)]
			public int Successes;

			[FieldOffset(8)]
			public long Erros;

			[FieldOffset(16)]
			public bool IsValid;

			[FieldOffset(17)]
			public DateTime IndexedAt;
		}

		[StructLayout(LayoutKind.Explicit)]
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

		[Fact]
		public void ShouldThrowIfStructDoesntHaveExplicitLayout()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Assert.Throws<InvalidDataException>(() => tx.State.Root.Write("item", new StructWithoutExplicitLayout()));
				Assert.Throws<InvalidDataException>(() => tx.State.Root.Read<StructWithoutExplicitLayout>("item"));
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
					IndexedAt = indexedAt
				});

				tree.Write("operations/1", new Operation()
				{
					Id = 1,
					Stats = new Stats
					{
						Attempts = 10,
						Successes = 10,
						IsValid = false,
						IndexedAt = indexedAt
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
				Assert.Equal(indexedAt, stats.IndexedAt);

				var operation = tree.Read<Operation>("operations/1").Value;

				Assert.Equal(1, operation.Id);
				Assert.Equal(10, operation.Stats.Successes);
				Assert.Equal(10, operation.Stats.Attempts);
				Assert.False(operation.Stats.IsValid);
				Assert.Equal(indexedAt, operation.Stats.IndexedAt);
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
	}
}