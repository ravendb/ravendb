// -----------------------------------------------------------------------
//  <copyright file="AbstractBaseIndexCodec.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;
using System.IO;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public abstract class AbstractBaseIndexCodec
	{
		public abstract Stream Encode(string key, Stream dataStream);

		public abstract Stream Decode(string key, Stream dataStream);
	}
}