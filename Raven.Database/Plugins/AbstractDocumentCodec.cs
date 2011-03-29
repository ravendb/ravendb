//-----------------------------------------------------------------------
// <copyright file="AbstractDocumentCodec.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Raven.Json.Linq;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public abstract class AbstractDocumentCodec
	{
		public abstract byte[] Encode(string key, RavenJObject data, RavenJObject metadata, byte[] bytes);

		public abstract byte[] Decode(string key, RavenJObject metadata, byte[] bytes);
	}
}
