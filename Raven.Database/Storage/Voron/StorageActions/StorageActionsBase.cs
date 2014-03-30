// -----------------------------------------------------------------------
//  <copyright file="StorageActionsBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Text;

using Raven.Abstractions.Util.Encryptors;
using Raven.Abstractions.Util.Streams;
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
	    private readonly Reference<SnapshotReader> snapshotReference;

	    private readonly IBufferPool bufferPool;

	    protected SnapshotReader Snapshot
	    {
	        get
	        {
	            return snapshotReference.Value;
	        }
	    }

	    protected StorageActionsBase(Reference<SnapshotReader> snapshotReference, IBufferPool bufferPool)
		{
		    this.snapshotReference = snapshotReference;
		    this.bufferPool = bufferPool;
		}

		protected string CreateKey(params object[] values)
		{
			if (values == null || values.Length == 0)
				throw new InvalidOperationException("Cannot create an empty key.");

		    if (values.Length == 1)
		        return values[0].ToString().ToLowerInvariant();

		    var sb = new StringBuilder();
			for (var i = 0; i < values.Length; i++)
			{
				var value = values[i];
			    sb.Append(value.ToString().ToLowerInvariant());
			    if (i < values.Length - 1)
			        sb.Append("/");
			}

		    return sb.ToString();
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