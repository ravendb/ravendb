// -----------------------------------------------------------------------
//  <copyright file="Index.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage.Voron.Impl
{
	public class Index : TableBase
	{
		public Index(string indexName)
			: base(indexName)
		{
		}
	}
}