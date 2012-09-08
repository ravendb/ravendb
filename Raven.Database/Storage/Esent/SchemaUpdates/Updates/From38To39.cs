// -----------------------------------------------------------------------
//  <copyright file="From37To38.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Impl;

namespace Raven.Storage.Esent.SchemaUpdates.Updates
{
	public class From38To39 : ISchemaUpdate
	{
		public string FromSchemaVersion { get { return "3.8"; } }

		public void Init(IUuidGenerator generator)
		{
		}

		public void Update(Session session, JET_DBID dbid)
		{
			using(var tasks = new Table(session, dbid, "tasks",OpenTableGrbit.None))
			{
				Api.JetDeleteIndex(session, tasks, "mergables_by_task_type");

				Api.JetDeleteColumn(session, tasks, "supports_merging");

				SchemaCreator.CreateIndexes(session, tasks, new JET_INDEXCREATE
				{
					szIndexName = "by_index_and_task_type",
					szKey = "+for_index\0+task_type\0\0",
					grbit = CreateIndexGrbit.IndexIgnoreNull,
				});
			}

			SchemaCreator.UpdateVersion(session, dbid, "3.9");
		}
	}
}