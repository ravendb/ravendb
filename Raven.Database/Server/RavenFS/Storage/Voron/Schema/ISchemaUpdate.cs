// -----------------------------------------------------------------------
//  <copyright file="ISchemaUpdate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.ComponentModel.Composition;

using Raven.Database.Server.RavenFS.Storage.Voron.Impl;

namespace Raven.Database.Server.RavenFS.Storage.Voron.Schema
{
	[InheritedExport]
	public interface ISchemaUpdate
	{
		string FromSchemaVersion { get; }

		string ToSchemaVersion { get; }

		void Update(TableStorage tableStorage, Action<string> output);

		void UpdateSchemaVersion(TableStorage tableStorage, Action<string> output);
	}
}