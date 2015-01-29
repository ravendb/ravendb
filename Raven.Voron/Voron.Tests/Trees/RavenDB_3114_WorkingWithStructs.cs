// -----------------------------------------------------------------------
//  <copyright file="AddingStructValues.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using System.IO;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Trees
{
	public class RavenDB_3114_WorkingWithStructs : StorageTest
	{
		class Foo
		{
			 
		}

		[Fact]
		public void ShouldNotAllowToCreateSchemaWithAnyTypeOfFields()
		{
			Assert.DoesNotThrow(() => new StructureSchema<string>());
			Assert.DoesNotThrow(() => new StructureSchema<MappedResults>());
			Assert.DoesNotThrow(() => new StructureSchema<int>());
			Assert.DoesNotThrow(() => new StructureSchema<ushort>());
			Assert.DoesNotThrow(() => new StructureSchema<byte>());

			var ae = Assert.Throws<ArgumentException>(() => new StructureSchema<Foo>());

			Assert.Equal("Structure schema can have fields of the following types: string, enum, primitives.", ae.Message);
		}

		[Fact]
		public void ShouldNotAllowToDefineBoolBecauseItsNonBlittable()
		{
			var ae = Assert.Throws<ArgumentException>(() => new StructureSchema<string>().Add<bool>("IsValid"));

			Assert.Equal("bool is the non-blittable type", ae.Message);
		}

		[Fact]
		public void ShouldNotAllowToDefineFixedSizeFieldAfterVariableSizeField()
		{
			var ae = Assert.Throws<ArgumentException>(() => new StructureSchema<string>().Add<string>("Message").Add<int>("Count"));

			Assert.Equal("Cannot define a fixed size field after variable size fields", ae.Message);
		}

		[Fact]
		public void ShouldThrowOnUnsupportedType()
		{
			var notSupportedException = Assert.Throws<NotSupportedException>(() => new StructureSchema<string>().Add<DateTime>("DateTime"));
			Assert.Equal("Not supported structure field type: System.DateTime", notSupportedException.Message);

			notSupportedException = Assert.Throws<NotSupportedException>(() => new StructureSchema<string>().Add<Foo>("Field"));
			Assert.True(notSupportedException.Message.StartsWith("Not supported structure field type:"));
		}

		[Fact]
		public void ShouldThrowWhenSettingUndefinedField()
		{
			var schema = new StructureSchema<string>()
				.Add<int>("Count")
				.Add<string>("Message");

			var structure = new Structure<string>(schema);

			Assert.DoesNotThrow(() => structure.Set("Count", 1));
			Assert.DoesNotThrow(() => structure.Set("Message", "hello"));

			var ae = Assert.Throws<ArgumentException>(() => structure.Set("does_not_exist", 1));
			Assert.Equal("No such field in schema defined. Field name: does_not_exist", ae.Message);

			ae = Assert.Throws<ArgumentException>(() => structure.Set("does_not_exist", "hello"));
			Assert.Equal("No such field in schema defined. Field name: does_not_exist", ae.Message);
		}

		[Fact]
		public void ShouldThrowWhenSettingDifferentValueTypeThanDefinedInSchema()
		{
			var schema = new StructureSchema<string>()
				.Add<int>("Count");

			var structure = new Structure<string>(schema);

			var invalidData = Assert.Throws<InvalidDataException>(() => structure.Set("Count", (long) 1));
			Assert.Equal("Attempt to set a field value which type is different than defined in the structure schema. Expected: System.Int32, got: System.Int64", invalidData.Message);
		}

		[Fact]
		public void ShouldThrowOnAttemptToPartialWriteOfVariableFields()
		{
			var schema = new StructureSchema<string>()
				.Add<string>("Message")
				.Add<string>("AdditionalInfo");

			var structure = new Structure<string>(schema);

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "stats");

				structure.Set("Message", "hello");

				var invalidOperationException = Assert.Throws<InvalidOperationException>(() => tree.WriteStruct("items/1", structure));

				Assert.Equal("Your structure has variable size fields. You have to set all of them to properly write a structure and avoid overlapping fields. Missing fields: AdditionalInfo", invalidOperationException.Message);
			}
		}

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
		public void CanWriteStructsByUsingWriteBatchAndReadFromSnapshot()
		{
			var statsSchema = new StructureSchema<string>()
				.Add<int>("Attempts")
				.Add<int>("Errors")
				.Add<int>("Successes")
				.Add<string>("Message");

			var operationSchema = new StructureSchema<string>()
				.Add<int>("Id")
				.Add<int>("Stats.Attempts")
				.Add<int>("Stats.Successes")
				.Add<string>("Message");

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
				.Set("Successes", 4)
				.Set("Message", "hello world"),
				"stats");

			batch.AddStruct("operations/1", 
				new Structure<string>(operationSchema)
				.Set("Id", 1)
				.Set("Stats.Attempts", 10)
				.Set("Stats.Successes", 10)
				.Set("Message", "hello world"),
				"operations");

			using (var snapshot = Env.CreateSnapshot())
			{
				var stats = snapshot.ReadStruct("stats", "stats/1", statsSchema, batch).Reader;

				Assert.Equal(5, stats.ReadInt("Attempts"));
				Assert.Equal(-1, stats.ReadInt("Errors"));
				Assert.Equal(4, stats.ReadInt("Successes"));
				Assert.Equal("hello world", stats.ReadString("Message"));
			}

			Env.Writer.Write(batch);

			using (var snapshot = Env.CreateSnapshot())
			{
				var operation = snapshot.ReadStruct("operations", "operations/1", operationSchema).Reader;

				Assert.Equal(1, operation.ReadInt("Id"));
				Assert.Equal(10, operation.ReadInt("Stats.Attempts"));
				Assert.Equal(10, operation.ReadInt("Stats.Successes"));
				Assert.Equal("hello world", operation.ReadString("Message"));
			}

			batch.Delete("stats/1", "stats");

			using (var snapshot = Env.CreateSnapshot())
			{
				var stats = snapshot.ReadStruct("stats", "stats/1", statsSchema, batch);

				Assert.Null(stats);
			}
		}

		[Fact]
		public void CanReadStructsFromTreeIterator()
		{
			var statsSchema = new StructureSchema<string>()
				.Add<int>("Attempts")
				.Add<string>("Message");

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "stats");

				tree.WriteStruct("items/1", new Structure<string>(statsSchema)
					.Set("Attempts", 1)
					.Set("Message", "1"));

				tree.WriteStruct("items/2", new Structure<string>(statsSchema)
					.Set("Attempts", 2)
					.Set("Message", "2"));


				tree.WriteStruct("items/3", new Structure<string>(statsSchema)
					.Set("Attempts", 3)
					.Set("Message", "3"));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var iterator = tx.ReadTree("stats").Iterate();

				iterator.Seek(Slice.BeforeAllKeys);

				var count = 0;

				do
				{
					var stats = iterator.ReadStructForCurrent(statsSchema);

					count++;

					Assert.Equal(count, stats.ReadInt("Attempts"));
					Assert.Equal(count.ToString(CultureInfo.InvariantCulture), stats.ReadString("Message"));

				} while (iterator.MoveNext());

				Assert.Equal(3, count);
			}
		}

		public enum MappedResults
		{
			View,
			ReduceKey,
			Bucket,
			DocId,
			Etag,
			TimestampBinary
		}

		[Fact]
		public void BasicStructureWithStringTest()
		{
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

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var readTree = tx.ReadTree("stats");

				var mappedResults = readTree.ReadStruct("items/1", schema).Reader;

				Assert.Equal(3, mappedResults.ReadInt(MappedResults.View));
				Assert.Equal("reduce_key", mappedResults.ReadString(MappedResults.ReduceKey));
			}
		}

		[Fact]
		public void MultipleStringFieldsShouldBeWrittenInProperOrder()
		{
			var schema = new StructureSchema<string>()
				.Add<string>("Hello")
				.Add<string>("World");

			var structure = new Structure<string>(schema);

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "stats");

				structure.Set("World", "W0rld!"); // set 2nd value first
				structure.Set("Hello", "Hell0"); // set 1st value in second operation

				tree.WriteStruct("items/1", structure);

				var structReader = tree.ReadStruct("items/1", schema).Reader;

				Assert.Equal("Hell0", structReader.ReadString("Hello"));
				Assert.Equal("W0rld!", structReader.ReadString("World"));
			}
		}

		[Fact]
		public void ComplexStructureTest()
		{
			var now = DateTime.Now;
			var etag = new byte[16]
			{
				1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6
			};

			var schema = new StructureSchema<MappedResults>()
				.Add<int>(MappedResults.View)
				.Add<long>(MappedResults.Bucket)
				.Add<long>(MappedResults.TimestampBinary)
				.Add<string>(MappedResults.ReduceKey)
				.Add<string>(MappedResults.DocId)
				.Add<byte[]>(MappedResults.Etag);


			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "stats");

				tree.WriteStruct("items/1",
					new Structure<MappedResults>(schema)
						.Set(MappedResults.View, 3)
						.Set(MappedResults.Bucket, 999L)
						.Set(MappedResults.TimestampBinary, now.ToBinary())
						.Set(MappedResults.ReduceKey, "reduce_key")
						.Set(MappedResults.DocId, "orders/1")
						.Set(MappedResults.Etag, etag));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var readTree = tx.ReadTree("stats");

				var mappedResults = readTree.ReadStruct("items/1", schema).Reader;

				Assert.Equal(3, mappedResults.ReadInt(MappedResults.View));
				Assert.Equal("reduce_key", mappedResults.ReadString(MappedResults.ReduceKey));
				Assert.Equal(999, mappedResults.ReadLong(MappedResults.Bucket));
				Assert.Equal("orders/1", mappedResults.ReadString(MappedResults.DocId));
				Assert.Equal(etag, mappedResults.ReadBytes(MappedResults.Etag));
				Assert.Equal(now, DateTime.FromBinary(mappedResults.ReadLong(MappedResults.TimestampBinary)));
			}
		}
	}
}