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
		public DocumentDatabase Database { get; set; }

		public virtual void Initialize(DocumentDatabase database)
		{
			Database = database;
			Initialize();
		}

		public virtual void Initialize()
		{

		}

		public virtual void SecondStageInit()
		{

		}


		public abstract Stream Encode(string key, RavenJObject data, RavenJObject metadata, Stream dataStream);

		public abstract Stream Decode(string key, RavenJObject metadata, Stream dataStream);
	}
}
