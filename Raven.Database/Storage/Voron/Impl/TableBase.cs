// -----------------------------------------------------------------------
//  <copyright file="TableBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage.Voron.Impl
{
	using System;
	using System.IO;

	using Raven.Abstractions.Extensions;
	using Raven.Database.Util.Streams;
	using Raven.Json.Linq;

	using global::Voron;
	using global::Voron.Impl;
	using global::Voron.Trees;

	public abstract class TableBase
	{
		protected string TableName { get; private set; }

		protected TableBase(string tableName)
		{
			if (string.IsNullOrEmpty(tableName))
				throw new ArgumentNullException(tableName);

			TableName = tableName;
		}

		public virtual void Add(WriteBatch writeBatch, string key, byte[] value)
		{
			AddOrUpdate(writeBatch, key, value, 0);
		}

		public virtual void Add(WriteBatch writeBatch, string key, Stream value)
		{
			AddOrUpdate(writeBatch, key, value, 0);
		}

		public virtual void Add(WriteBatch writeBatch, string key, RavenJToken value)
		{
			AddOrUpdate(writeBatch, key, value, 0);
		}

		public virtual void AddOrUpdate(WriteBatch writeBatch, string key, byte[] value, ushort? expectedVersion = null)
		{
			var stream = new MemoryStream(value);
			writeBatch.Add(key, stream, TableName, expectedVersion);
		}

		public virtual void AddOrUpdate(WriteBatch writeBatch, string key, Stream value, ushort? expectedVersion = null)
		{
			writeBatch.Add(key, value, TableName, expectedVersion);
		}

		public virtual void AddOrUpdate(WriteBatch writeBatch, string key, RavenJToken value, ushort? expectedVersion = null)
		{
			var stream = new MemoryStream();
			value.WriteTo(stream);
			stream.Position = 0;

			writeBatch.Add(key, stream, TableName, expectedVersion);
		}

		public virtual void MultiAdd(WriteBatch writeBatch, Slice key, Slice value, ushort? expectedVersion = null)
		{
			writeBatch.MultiAdd(key, value, TableName, expectedVersion);
		}

		public virtual ReadResult Read(SnapshotReader snapshot, Slice key)
		{
			return snapshot.Read(TableName, key);
		}

		public virtual IIterator MultiRead(SnapshotReader snapshot, Slice key)
		{
			return snapshot.MultiRead(TableName, key);
		}

		public virtual TreeIterator Iterate(SnapshotReader snapshot)
		{
			return snapshot.Iterate(TableName);
		}

		public bool Contains(SnapshotReader snapshot, string key)
		{
			return snapshot.ReadVersion(TableName, key) > 0;
		}

		public virtual void Delete(WriteBatch writeBatch, string key, ushort? expectedVersion = null)
		{
			writeBatch.Delete(key, TableName, expectedVersion);
		}

		public virtual void Delete(WriteBatch writeBatch, Slice key, ushort? expectedVersion = null)
		{
			writeBatch.Delete(key, TableName, expectedVersion);
		}

		public virtual void MultiDelete(WriteBatch writeBatch, Slice key, Slice value, ushort? expectedVersion = null)
		{
			writeBatch.MultiDelete(key, value, TableName, expectedVersion);
		}
	}
}