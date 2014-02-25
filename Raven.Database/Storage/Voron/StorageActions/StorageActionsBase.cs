// -----------------------------------------------------------------------
//  <copyright file="StorageActionsBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Text;

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

		protected string CreateLowercasedKey(params object[] values)
		{
			return CreateKeyInternal(true, values);
		}

		protected string CreateKey(params object[] values)
		{
			return CreateKeyInternal(false, values);
		}

		private string CreateKeyInternal(bool isLowerCasedKey, params object[] values)
		{
			if (values == null || values.Length == 0)
				throw new InvalidOperationException("Cannot create an empty key.");

			if (values.Length == 1)
				return LowercasedKey(isLowerCasedKey, values[0].ToString());

			var sb = new StringBuilder();
			for (var i = 0; i < values.Length; i++)
			{
				var value = values[i];
				sb.Append(LowercasedKey(isLowerCasedKey, value.ToString()));
			    if (i < values.Length - 1)
			        sb.Append("/");
			}

		    return sb.ToString();
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