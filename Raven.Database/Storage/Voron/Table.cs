// -----------------------------------------------------------------------
//  <copyright file="Table.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage.Voron
{
	using System.IO;

	using global::Voron.Impl;
	using global::Voron.Trees;

	public class Table
	{
		private readonly Tree tree;

		public Table(Tree tree)
		{
			this.tree = tree;
		}

		public void Add(Transaction tx, string key, byte[] value)
		{
			using (var stream = new MemoryStream(value))
			{
				Add(tx, key, stream);
			}
		}

		public void Add(Transaction tx, string key, Stream value)
		{
			tree.Add(tx, key, value);
		}

		public Stream Read(Transaction tx, string key)
		{
			return tree.Read(tx, key);
		}

		public void Delete(Transaction tx, string key)
		{
			tree.Delete(tx, key);
		}
	}
}