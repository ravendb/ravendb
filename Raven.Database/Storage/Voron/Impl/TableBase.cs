// -----------------------------------------------------------------------
//  <copyright file="TableBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage.Voron.Impl
{
	using System;
	using System.IO;

	using global::Voron.Impl;

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
			var stream = new MemoryStream(value);
			writeBatch.Add(key, stream, TableName);
		}

		public virtual void Add(WriteBatch writeBatch, string key, Stream value)
		{
			writeBatch.Add(key, value, TableName);
		}

		public virtual Stream Read(SnapshotReader snapshot, string key)
		{
			return snapshot.Read(TableName, key);
		}

		public bool Contains(SnapshotReader snapshot, string key)
		{
			using (var resultStream = Read(snapshot, key))
				return resultStream != null;
		}

		public virtual void Delete(WriteBatch writeBatch, string key)
		{
			writeBatch.Delete(key, this.TableName);
		}
	}
}