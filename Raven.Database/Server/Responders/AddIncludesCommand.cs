//-----------------------------------------------------------------------
// <copyright file="AddIncludesCommand.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Json;
using Raven.Http;

namespace Raven.Database.Server.Responders
{
	public class AddIncludesCommand
	{

		public AddIncludesCommand(
			DocumentDatabase database, 
			TransactionInformation transactionInformation, 
			Action<Guid, JObject> add, 
            string[] includes,
			HashSet<string> loadedIds)
		{
			Add = add;
			Includes = includes;
			Database = database;
			TransactionInformation = transactionInformation;
			LoadedIds = loadedIds;
		}

		private string[] Includes { get; set; }

		private Action<Guid,JObject> Add { get; set; }

		private DocumentDatabase Database { get; set; }

		private TransactionInformation TransactionInformation { get; set; }

		private HashSet<string> LoadedIds { get; set; }

		public void Execute(JObject document)
		{
			foreach (var include in Includes)
			{
			    foreach (var token in document.SelectTokenWithRavenSyntax(include))
			    {
			        ExecuteInternal(token);
			    }
			}
		}

		private void ExecuteInternal(JToken token)
		{
			switch (token.Type)
			{
				case JTokenType.Array:
					foreach (var item in (JArray)token)
					{
						ExecuteInternal(item);
					}
					break;
				case JTokenType.String:
					var value = token.Value<string>();
					if (LoadedIds.Add(value) == false)
						return;
					var includedDoc = Database.Get(value, TransactionInformation);
					if (includedDoc != null)
						Add(includedDoc.Etag,includedDoc.ToJson());
					break;
				default:
					// here we ignore everything else
					// if it ain't a string or array, it is invalid
					// as an id
					break;
			}
		}
	}
}
