// -----------------------------------------------------------------------
//  <copyright file="Index.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Util.Streams;
using Raven.Database.Util.Streams;

namespace Raven.Database.Storage.Voron.Impl
{
	public class Index : TableBase
	{
		public Index(string indexName, IBufferPool bufferPool)
			: base(indexName, bufferPool)
		{
		}
	}
}