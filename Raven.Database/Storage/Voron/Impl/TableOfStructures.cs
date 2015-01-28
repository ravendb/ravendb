// -----------------------------------------------------------------------
//  <copyright file="TableOfStructures.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Util.Streams;
using Voron;
using Voron.Impl;

namespace Raven.Database.Storage.Voron.Impl
{
	internal class TableOfStructures<T> : Table
	{
		public readonly StructureSchema<T> Schema;

		public TableOfStructures(string tableName, StructureSchema<T> schema, IBufferPool bufferPool, params string[] indexNames) : base(tableName, bufferPool, indexNames)
		{
			Schema = schema;
		}

		public virtual StructReadResult<T> ReadStruct(SnapshotReader snapshot, Slice key, WriteBatch writeBatch)
		{
			return snapshot.ReadStruct(TableName, key, Schema, writeBatch);
		}

		public virtual void AddStruct(WriteBatch writeBatch, Slice key, Structure value, ushort? expectedVersion = null, bool shouldIgnoreConcurrencyExceptions = false)
		{
			writeBatch.AddStruct(key, value, TableName, expectedVersion, shouldIgnoreConcurrencyExceptions);
		}
	}
}