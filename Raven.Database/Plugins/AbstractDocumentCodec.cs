//-----------------------------------------------------------------------
// <copyright file="AbstractDocumentCodec.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public abstract class AbstractDocumentCodec
	{
		public abstract byte[] Encode(string key, JObject data, JObject metadata, byte[] bytes);

		public abstract byte[] Decode(string key, JObject metadata, byte[] bytes);
	}
}
