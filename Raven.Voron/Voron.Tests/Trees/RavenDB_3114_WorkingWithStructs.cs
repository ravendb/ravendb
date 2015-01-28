// -----------------------------------------------------------------------
//  <copyright file="AddingStructValues.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Trees
{
	public class RavenDB_3114_WorkingWithStructs : StorageTest
	{

		[Fact]
		public void CanReadAndWriteStructsFromTrees()
		{
			var indexedAt = new DateTime(2015, 1, 20);

			var schema = new StructureSchema<string>()
				.Add<int>("Attempts")
				.Add<int>("Errors")
				.Add<int>("Successes")
				.Add<byte>("IsValid")
				.Add<long>("IndexedAt");

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "stats");

				var stats = new Structure<string>(schema);

				stats.Set("Attempts", 5);
				stats.Set("Errors", -1);
				stats.Set("Successes", 4);
				stats.Set("IsValid", (byte) 1);
				stats.Set("IndexedAt", indexedAt.ToBinary());

				tree.WriteStruct("stats/1", stats);

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var tree = tx.ReadTree("stats");

				var stats = tree.ReadStruct("stats/1", schema).Reader;

				Assert.Equal(5, stats.ReadInt("Attempts"));
				Assert.Equal(-1, stats.ReadInt("Errors"));
				Assert.Equal(4, stats.ReadInt("Successes"));
				Assert.Equal(1, stats.ReadByte("IsValid"));
				Assert.Equal(indexedAt, DateTime.FromBinary(stats.ReadLong("IndexedAt")));
			}
		}

		[Fact]
		public void CanDeleteStructsFromTrees()
		{
			var schema = new StructureSchema<string>()
				.Add<int>("Attempts")
				.Add<int>("Errors")
				.Add<int>("Successes");

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "stats");

				var stats = new Structure<string>(schema);

				stats.Set("Attempts", 5);
				stats.Set("Errors", -1);
				stats.Set("Successes", 4);

				tree.WriteStruct("stats/1", stats);

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = tx.ReadTree("stats");

				tree.Delete("stats/1");

				var stats = tree.ReadStruct("stats/1", schema);

				Assert.Null(stats);
			}
		}

		[Fact]
		public void CanWriteStructsByUsingWriteBatch()
		{
			var statsSchema = new StructureSchema<string>()
				.Add<int>("Attempts")
				.Add<int>("Errors")
				.Add<int>("Successes");

			var operationSchema = new StructureSchema<string>()
				.Add<int>("Id")
				.Add<int>("Stats.Attempts")
				.Add<int>("Stats.Successes");

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "stats");
				Env.CreateTree(tx, "operations");

				tx.Commit();
			}
			var batch = new WriteBatch();

			batch.AddStruct("stats/1", 
				new Structure<string>(statsSchema)
				.Set("Attempts", 5)
				.Set("Errors", -1)
				.Set("Successes", 4),
				"stats");

			batch.AddStruct("operations/1", 
				new Structure<string>(operationSchema)
				.Set("Id", 1)
				.Set("Stats.Attempts", 10)
				.Set("Stats.Successes", 10),
				"operations");

			using (var snapshot = Env.CreateSnapshot())
			{
				var stats = snapshot.ReadStruct("stats", "stats/1", statsSchema, batch).Reader;

				Assert.Equal(5, stats.ReadInt("Attempts"));
				Assert.Equal(-1, stats.ReadInt("Errors"));
				Assert.Equal(4, stats.ReadInt("Successes"));
			}

			Env.Writer.Write(batch);

			using (var snapshot = Env.CreateSnapshot())
			{
				var operation = snapshot.ReadStruct("operations", "operations/1", operationSchema).Reader;

				Assert.Equal(1, operation.ReadInt("Id"));
				Assert.Equal(10, operation.ReadInt("Stats.Attempts"));
				Assert.Equal(10, operation.ReadInt("Stats.Successes"));
			}

			batch.Delete("stats/1", "stats");

			using (var snapshot = Env.CreateSnapshot())
			{
				var stats = snapshot.ReadStruct("stats", "stats/1", statsSchema, batch);

				Assert.Null(stats);
			}
		}

		//[Fact]
		//public void CanReadStructsFromTreeIterator()
		//{
		//	using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
		//	{
		//		var tree = Env.CreateTree(tx, "stats");

		//		tree.Write("items/1", new Stats()
		//		{
		//			Attempts = 1
		//		});

		//		tree.Write("items/2", new Stats()
		//		{
		//			Attempts = 2
		//		});


		//		tree.Write("items/3", new Stats()
		//		{
		//			Attempts = 3
		//		});

		//		tx.Commit();
		//	}

		//	using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
		//	{
		//		var iterator = tx.ReadTree("stats").Iterate();

		//		iterator.Seek(Slice.BeforeAllKeys);

		//		var count = 0;

		//		do
		//		{
		//			var stats = iterator.ReadStructForCurrent<Stats>();

		//			count++;

		//			Assert.Equal(count, stats.Attempts);

		//		} while (iterator.MoveNext());

		//		Assert.Equal(3, count);
		//	}
		//}

		public enum MappedResults
		{
			View,
			ReduceKey,
			Bucket,
			DocId,
			Etag,
			TimestampTicks
		}

		[Fact]
		public void StructuresCanHaveStrings()
		{
			var now = DateTime.Now;
			var etag = new byte[16]
			{
				1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6
			};

			var schema = new StructureSchema<Enum>()
				.Add<int>(MappedResults.View)
				.Add<string>(MappedResults.ReduceKey);


			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "stats");

				tree.WriteStruct("items/1", 
					new Structure<Enum>(schema)
						.Set(MappedResults.View, 3)
						.Set(MappedResults.ReduceKey, "reduce_key"));

				//{
				//	View = 3,
				//	ReduceKey = "reduce_key",
				//	Bucket = 1024,
				//	DocId = "orders/1",
				//	Etag = etag,
				//	TimestampTicks = now.Ticks
				//}

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var readTree = tx.ReadTree("stats");

				var mappedResults = readTree.ReadStruct("items/1", schema).Reader;

				Assert.Equal(3, mappedResults.ReadInt(MappedResults.View));
				Assert.Equal("reduce_key", mappedResults.ReadString(MappedResults.ReduceKey));
				//Assert.Equal(1024, mappedResults.Bucket);
				//Assert.Equal("orders/1", mappedResults.DocId);
				//Assert.Equal(etag, mappedResults.Etag);
				//Assert.Equal(now, new DateTime(mappedResults.TimestampTicks));
			}
		}
	}
}