//-----------------------------------------------------------------------
// <copyright file="IRequiresDocumentDatabaseInitialization.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;

namespace Raven.Database.Plugins
{
	internal interface IRequiresDocumentDatabaseInitialization
	{
		void Initialize(DocumentDatabase database);

		void SecondStageInit();
	}
}
