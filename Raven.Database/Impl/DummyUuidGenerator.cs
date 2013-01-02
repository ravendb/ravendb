//-----------------------------------------------------------------------
// <copyright file="DummyUuidGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;

namespace Raven.Database.Impl
{
	public class DummyUuidGenerator : IUuidGenerator
	{
		private byte cur;
		public Etag CreateSequentialUuid(UuidType type)
		{
			var bytes = new byte[16];
			bytes[15] += ++cur;
			return Etag.Parse(bytes);
		}
	}
}
