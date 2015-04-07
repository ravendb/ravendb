// -----------------------------------------------------------------------
//  <copyright file="StorageActionsBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Runtime.InteropServices;
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
    using System.Globalization;
    using System.Runtime.CompilerServices;

	internal abstract class StorageActionsBase
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

        private static readonly CultureInfo Invariant = CultureInfo.InvariantCulture;

        #region High-performance key management

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static string AppendToKey<T, U, W>(string key, T item1, U item2, W item3)
        {
            return AppendToKey(key, item1.ToString(), item2.ToString(), item3.ToString());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static string AppendToKey<T, U>(string key, T item1, U item2)
        {
            return AppendToKey(key, item1.ToString(), item2.ToString());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static string AppendToKey<T>(string key, T value)
        {
            return key + "/" + value.ToString().ToLower(Invariant);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static string AppendToKey(string key, string value)
        {
            return key + "/" + value.ToLower(Invariant);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static string AppendToKey(string key, string item1, string item2)
        {
            return key + "/" + item1.ToLower(Invariant) + "/" + item2.ToLower(Invariant);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static string AppendToKey(string key, string item1, string item2, string item3)
        {
            return key + "/" + item1.ToLower(Invariant) + "/" + item2.ToLower(Invariant) + "/" + item3.ToLower(Invariant);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static string CreateKey<T, U, W, X>(T item1, U item2, W item3, X item4 )
        {
            if (item1 == null || item2 == null || item3 == null || item4 == null)
                throw new InvalidOperationException("Cannot create an empty key.");

            return CreateKey(item1.ToString(), item2.ToString(), item3.ToString(), item4.ToString());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static string CreateKey<T, U, W>(T item1, U item2, W item3)
        {
            if (item1 == null || item2 == null || item3 == null)
                throw new InvalidOperationException("Cannot create an empty key.");

            return CreateKey(item1.ToString(), item2.ToString(), item3.ToString());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static string CreateKey<T, U>(T item1, U item2)
        {
            if (item1 == null || item2 == null)
                throw new InvalidOperationException("Cannot create an empty key.");

            return CreateKey(item1.ToString(), item2.ToString());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static string CreateKey<T>(T value)
        {
            if (value == null)
                throw new InvalidOperationException("Cannot create an empty key.");

            return value.ToString().ToLower(Invariant);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static string CreateKey(string value)
        {
            return value.ToLower(Invariant);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static string CreateKey(string item1, string item2)
        {
            return item1.ToLower(Invariant) + "/" + item2.ToLower(Invariant);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static string CreateKey(string item1, string item2, string item3)
        {
            return item1.ToLower(Invariant) + "/" + item2.ToLower(Invariant) + "/" + item3.ToLower(Invariant);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static string CreateKey(string item1, string item2, string item3, string item4)
        {
            return item1.ToLower(Invariant) + "/" + item2.ToLower(Invariant) + "/" + item3.ToLower(Invariant) + "/" + item4.ToLower(Invariant);
        }

        #endregion

        protected static string AppendToKey(string key, params string[] values)
        {
            if (values == null || values.Length == 0)
                throw new InvalidOperationException("Cannot append an empty key.");

            if (values.Length == 1)
                return key + "/" + values[0].ToLower(Invariant);

            var sb = new StringBuilder(key);
            sb.Append("/");
            for (var i = 0; i < values.Length; i++)
            {
                var value = values[i];
                sb.Append(value.ToLower(Invariant));
                if (i < values.Length - 1)
                    sb.Append("/");
            }

            return sb.ToString();
        }

        protected string CreateKey(params string[] values)
        {
            if (values == null || values.Length == 0)
                throw new InvalidOperationException("Cannot create an empty key.");

            if (values.Length == 1)
                return values[0].ToLower(Invariant);

            var sb = new StringBuilder();
            for (var i = 0; i < values.Length; i++)
            {
                var value = values[i];
                sb.Append(value.ToLower(Invariant));
                if (i < values.Length - 1)
                    sb.Append("/");
            }

            return sb.ToString();
        }
       
        protected string CreateKey(params object[] values)
        {
            if (values == null || values.Length == 0)
                throw new InvalidOperationException("Cannot create an empty key.");

            if (values.Length == 1)
                return values[0].ToString().ToLower(Invariant);

            var sb = new StringBuilder();
            for (var i = 0; i < values.Length; i++)
            {
                var value = values[i];
                sb.Append(value.ToString().ToLower(Invariant));
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

		protected StructureReader<T> LoadStruct<T>(TableOfStructures<T> table, Slice key, WriteBatch writeBatch, out ushort version)
		{
			var read = table.ReadStruct(Snapshot, key, writeBatch);
			if (read == null)
			{
				version = 0;
				return null;
			}

			version = read.Version;
			return read.Reader;
		}

        protected BufferPoolMemoryStream CreateStream()
        {
            return new BufferPoolMemoryStream(bufferPool);
        }
	}
}