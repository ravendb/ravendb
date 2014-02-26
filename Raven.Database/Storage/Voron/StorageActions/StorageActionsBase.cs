// -----------------------------------------------------------------------
//  <copyright file="StorageActionsBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Text;
using System.Web.UI.WebControls;
using Raven.Database.Util.Streams;

namespace Raven.Database.Storage.Voron.StorageActions
{
	using System;

	using Raven.Abstractions.Extensions;
	using Raven.Database.Storage.Voron.Impl;
	using Raven.Json.Linq;

	using global::Voron;
	using global::Voron.Impl;

	public abstract class StorageActionsBase
	{
	    private readonly IBufferPool bufferPool;

		protected SnapshotReader Snapshot { get; private set; }

		protected StorageActionsBase(SnapshotReader snapshot, IBufferPool bufferPool)
		{
		    this.bufferPool = bufferPool;
			Snapshot = snapshot;
		}

		protected Slice CreateLowercasedKey(params object[] values)
		{
			return CreateKeyInternal(true, values);
		}

		protected Slice CreateKey(params object[] values)
		{
			return CreateKeyInternal(false, values);
		}

		private Slice CreateKeyInternal(bool isLowerCasedKey, params object[] values)
		{
			if (values == null || values.Length == 0)
				throw new InvalidOperationException("Cannot create an empty key.");

			var list = new List<byte>();
			for (var i = 0; i < values.Length; i++)
			{
				var value = values[i];

				var bytes = value as byte[];
				list.AddRange(bytes ?? Encoding.UTF8.GetBytes(LowercasedKey(isLowerCasedKey, value.ToString())));

				if (i < values.Length - 1)
			        list.Add((byte)'/');
			}

		    return new Slice(list.ToArray());
		}

		private string LowercasedKey(bool isLowerCasedKey, string key)
		{
			return isLowerCasedKey ? key.ToLowerInvariant() : key;
		}

		protected RavenJObject LoadJson(Table table, Slice key, WriteBatch writeBatch, out ushort version)
		{
			var read = table.Read(Snapshot, key, writeBatch);
			if (read == null)
			{
				version = 0;
				return null;
			} 
			
			using (var stream = read.Reader.AsStream())
			{
				version = read.Version;
				return stream.ToJObject();
			}
		}

        protected BufferPoolMemoryStream CreateStream()
        {
            return new BufferPoolMemoryStream(bufferPool);
        }
	}
}