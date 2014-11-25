//-----------------------------------------------------------------------
// <copyright file="AbstractDocumentCodec.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public abstract class AbstractIndexCodec : AbstractBaseIndexCodec, IRequiresDocumentDatabaseInitialization
	{
		public virtual void Initialize(DocumentDatabase database)
		{
		}


		public virtual void SecondStageInit()
		{

		}
	}
}
