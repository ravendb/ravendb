// -----------------------------------------------------------------------
//  <copyright file="Table.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Voron.Trees;

namespace Raven.Database.Storage.Voron
{
	using System.IO;

	using global::Voron.Impl;	

	public class Table
	{
		protected readonly string treeName;

        public Table(string treeName)
		{
            if (String.IsNullOrWhiteSpace(treeName))
            {
                throw new ArgumentNullException(treeName);
            }

			this.treeName = treeName;
		}

		public void Add(WriteBatch writeBatch, string key, byte[] value)
		{
		    var stream = new MemoryStream(value);
            writeBatch.Add(key, stream, treeName);
        }

        public void Add(WriteBatch writeBatch, string key, Stream value)
		{
            writeBatch.Add(key, value, treeName);
		}

        public Stream Read(SnapshotReader snapshot, string key)
        {
            return snapshot.Read(treeName, key);
        }

	    public bool Contains(SnapshotReader snapshot, string key)
	    {
	        using (var resultStream = Read(snapshot, key))
	        {
	            return resultStream != null;
	        }
	    }

        public void Delete(WriteBatch writeBatch, string key)
		{
            writeBatch.Delete(key, treeName);
		}
	}
}