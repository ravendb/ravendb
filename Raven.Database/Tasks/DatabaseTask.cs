//-----------------------------------------------------------------------
// <copyright file="Task.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Raven.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Database.Indexing;

namespace Raven.Database.Tasks
{
	public abstract class DatabaseTask
	{
		public int Index { get; set; }

        public abstract bool SeparateTasksByIndex { get; }

		public abstract void Merge(DatabaseTask task);
		public abstract void Execute(WorkContext context);

		public byte[] AsBytes()
		{
			var memoryStream = new MemoryStream();
			JsonExtensions.CreateDefaultJsonSerializer().Serialize(new BsonWriter(memoryStream), this);
			return memoryStream.ToArray();
		}

		public static DatabaseTask ToTask(string taskType, byte[] task)
		{
			var type = typeof(DatabaseTask).Assembly.GetType(taskType);
			return (DatabaseTask) JsonExtensions.CreateDefaultJsonSerializer().Deserialize(new BsonReader(new MemoryStream(task)), type);
		}

		public abstract DatabaseTask Clone();
	}
}
