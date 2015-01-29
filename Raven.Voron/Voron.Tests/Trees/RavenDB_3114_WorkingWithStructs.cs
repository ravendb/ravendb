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

		[Fact]
		public void CanWriteAndReadAllPrimitiveTypesAndDecimals()
		{
			var schema = new StructureSchema<string>()
				.Add<sbyte>("sbyte")
				.Add<byte>("byte")
				.Add<short>("short")
				.Add<ushort>("ushort")
				.Add<int>("int")
				.Add<uint>("uint")
				.Add<long>("long")
				.Add<ulong>("ulong")
				.Add<char>("char")
				.Add<float>("float")
				.Add<double>("double")
				.Add<decimal>("decimal")
				.Add<bool>("bool");

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "primitives");

				tree.WriteStruct("primitives/1",
					new Structure<string>(schema)
						.Set("sbyte", sbyte.MaxValue)
						.Set("byte", byte.MaxValue)
						.Set("short", short.MaxValue)
						.Set("ushort", ushort.MaxValue)
						.Set("int", int.MaxValue)
						.Set("uint", uint.MaxValue)
						.Set("long", long.MaxValue)
						.Set("ulong", ulong.MaxValue)
						.Set("char", char.MaxValue)
						.Set("float", float.MaxValue)
						.Set("double", double.MaxValue)
						.Set("decimal", decimal.MaxValue)
						.Set("bool", false));

				var primitives = tree.ReadStruct("primitives/1", schema).Reader;

				Assert.Equal(sbyte.MaxValue, primitives.ReadSByte("sbyte"));
				Assert.Equal(byte.MaxValue, primitives.ReadByte("byte"));
				Assert.Equal(short.MaxValue, primitives.ReadShort("short"));
				Assert.Equal(ushort.MaxValue, primitives.ReadUShort("ushort"));
				Assert.Equal(int.MaxValue, primitives.ReadInt("int"));
				Assert.Equal(uint.MaxValue, primitives.ReadUInt("uint"));
				Assert.Equal(long.MaxValue, primitives.ReadLong("long"));
				Assert.Equal(ulong.MaxValue, primitives.ReadULong("ulong"));
				Assert.Equal(char.MaxValue, primitives.ReadChar("char"));
				Assert.Equal(float.MaxValue, primitives.ReadFloat("float"));
				Assert.Equal(double.MaxValue, primitives.ReadDouble("double"));
				Assert.Equal(decimal.MaxValue, primitives.ReadDecimal("decimal"));
				Assert.Equal(false, primitives.ReadBool("bool"));


				tree.WriteStruct("primitives/1",
					new Structure<string>(schema)
						.Increment("sbyte", -1 * sbyte.MaxValue)
						.Increment("short", -1 * short.MaxValue)
						.Increment("int", -1 * int.MaxValue)
						.Increment("long", -1 * long.MaxValue));

				primitives = tree.ReadStruct("primitives/1", schema).Reader;

				Assert.Equal(0, primitives.ReadSByte("sbyte"));
				Assert.Equal(0, primitives.ReadShort("short"));
				Assert.Equal(0, primitives.ReadInt("int"));
				Assert.Equal(0, primitives.ReadLong("long"));
				

				tree.WriteStruct("primitives/2",
					new Structure<string>(schema)
						.Set("byte", (byte) 0)
						.Set("ushort", (ushort) 0)
						.Set("uint", (uint) 0)
						.Set("ulong", (ulong) 0)
						.Set("float", 0f)
						.Set("double", 0d)
						.Set("decimal", (decimal) 0)
						.Set("char", 'a'));

				tree.WriteStruct("primitives/2",
					new Structure<string>(schema)
						.Increment("byte", 1)
						.Increment("ushort", 2)
						.Increment("uint", 3)
						.Increment("ulong", 4)
						.Increment("float", -1)
						.Increment("double", -2)
						.Increment("decimal", -3)
						.Increment("char", 1));

				var primitives2 = tree.ReadStruct("primitives/2", schema).Reader;

				Assert.Equal(1, primitives2.ReadByte("byte"));
				Assert.Equal(2, primitives2.ReadUShort("ushort"));
				Assert.Equal((uint) 3, primitives2.ReadUInt("uint"));
				Assert.Equal((ulong) 4, primitives2.ReadULong("ulong"));
				Assert.Equal(-1, primitives2.ReadFloat("float"));
				Assert.Equal(-2, primitives2.ReadDouble("double"));
				Assert.Equal(-3, primitives2.ReadDecimal("decimal"));
				Assert.Equal('b', primitives2.ReadChar("char"));
			}
		}
	}
}