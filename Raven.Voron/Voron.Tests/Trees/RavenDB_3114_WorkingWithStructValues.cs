// -----------------------------------------------------------------------
//  <copyright file="AddingStructValues.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Runtime.InteropServices;
using Xunit;

namespace Voron.Tests.Trees
{
	public class RavenDB_3114_WorkingWithStructValues : StorageTest
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

			//public unsafe fixed byte Bytes[8];
		}

		[StructLayout(LayoutKind.Explicit)]
		public struct Operation
		{
			[FieldOffset(0)]
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
				Assert.Throws<InvalidDataException>(() => tx.State.Root.Write<StructWithoutExplicitLayout>("item", x => x.Field, 1));

				Assert.Throws<InvalidDataException>(() => tx.State.Root.Read<StructWithoutExplicitLayout>("item"));
				Assert.Throws<InvalidDataException>(() => tx.State.Root.Read<StructWithoutExplicitLayout, int>("item", x => x.Field));
			}
		}

		[Fact]
		public void CanReadAndWriteEntireStruct()
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

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var tree = tx.ReadTree("stats");

				var stats = tree.Read<Stats>("stats/1").Value;

				Assert.Equal(5, stats.Attempts);
				Assert.Equal(-1, stats.Erros);
				Assert.Equal(4, stats.Successes);
			}
		}

		[Fact]
		public void CanReadAndWriteStructValues()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "stats");

				tree.Write<Stats>("item", x => x.Successes, 10);
				tree.Write<Operation>("operations/1", x => x.Stats.Erros, 3);
				tree.Write<Operation>("operations/2", x => x.Stats, new Stats()
				{
					Attempts = 3,
					Erros = 2,
					Successes = 1
				});

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var tree = tx.ReadTree("stats");

				var successes = tree.Read<Stats, int>("item", x => x.Successes).Value;
				
				Assert.Equal(10, successes);

				var errors = tree.Read<Operation, long>("operations/1", x => x.Stats.Erros).Value;

				Assert.Equal(3, errors);

				var operationStats = tree.Read<Operation, Stats>("operations/2", x => x.Stats).Value;

				Assert.Equal(3, operationStats.Attempts);
				Assert.Equal(2, operationStats.Erros);
				Assert.Equal(1, operationStats.Successes);

				var stats = tree.Read<Stats>("item");

				Assert.Equal(10, stats.Value.Successes);

				var operation = tree.Read<Operation>("operations/1");

				Assert.Equal(3, operation.Value.Stats.Erros);
			}
		}
	}
}