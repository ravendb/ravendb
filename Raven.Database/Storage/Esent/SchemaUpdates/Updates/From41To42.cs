// -----------------------------------------------------------------------
//  <copyright file="From40To41.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Abstractions.Extensions;
using Raven.Database.Storage;
using Raven.Storage.Esent.StorageActions;

namespace Raven.Storage.Esent.SchemaUpdates.Updates
{
	public class From41To42 : ISchemaUpdate
	{
		public string FromSchemaVersion { get { return "4.1"; } }

		public void Init(IUuidGenerator generator, InMemoryRavenConfiguration configuration)
		{

		}

		public void Update(Session session, JET_DBID dbid, Action<string> output)
		{
			var defaultVal = BitConverter.GetBytes(0);
			Api.JetSetColumnDefaultValue(session, dbid, "reduce_keys_status", "reduce_type", defaultVal, defaultVal.Length,
			                             SetColumnDefaultValueGrbit.None);

			SchemaCreator.UpdateVersion(session, dbid, "4.2");
		}
	}
}