//-----------------------------------------------------------------------
// <copyright file="AbstractDocumentCodec.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using System.IO;
using Raven.Json.Linq;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public abstract class AbstractDocumentCodec : IRequiresDocumentDatabaseInitialization
	{
		public virtual void Initialize(DocumentDatabase database)
		{
		}

		public abstract Stream Encode(string key, RavenJObject data, RavenJObject metadata, Stream dataStream);

		public abstract Stream Decode(string key, RavenJObject metadata, Stream dataStream);
	}
}
