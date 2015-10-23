// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerStreamActionsBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

using Raven.Imports.Newtonsoft.Json;

namespace Raven.Smuggler.Database.Streams
{
	public class DatabaseSmugglerStreamActionsBase : IDisposable
	{
		protected JsonTextWriter Writer { get; private set; }

		public DatabaseSmugglerStreamActionsBase(JsonTextWriter writer, string sectionName)
		{
			Writer = writer;
			Writer.WritePropertyName(sectionName);
			Writer.WriteStartArray();
		}

		public void Dispose()
		{
			Writer.WriteEndArray();
		}
	}
}