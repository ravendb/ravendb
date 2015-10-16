// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerStreamActionsBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Raven.Imports.Newtonsoft.Json;

namespace Raven.Smuggler.Database.Impl.Streams
{
	public class DatabaseSmugglerStreamActionsBase : IDisposable
	{
		private readonly JsonTextWriter _writer;

		public DatabaseSmugglerStreamActionsBase(JsonTextWriter writer, string sectionName)
		{
			_writer = writer;
			_writer.WritePropertyName(sectionName);
			_writer.WriteStartArray();
		}

		public void Dispose()
		{
			_writer.WriteEndArray();
		}
	}
}