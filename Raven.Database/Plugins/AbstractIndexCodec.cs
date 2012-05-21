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
	public abstract class AbstractIndexCodec : IRequiresDocumentDatabaseInitialization
	{
		public virtual void Initialize(DocumentDatabase database)
		{
		}

		public abstract Stream Encode(string key, Stream dataStream);

		public abstract Stream Decode(string key, Stream dataStream);
	}
}
