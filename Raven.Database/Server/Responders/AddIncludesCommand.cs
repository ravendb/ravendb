//-----------------------------------------------------------------------
// <copyright file="AddIncludesCommand.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text.RegularExpressions;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders
{
	public class AddIncludesCommand
	{
		public AddIncludesCommand(
			DocumentDatabase database, 
			TransactionInformation transactionInformation, 
			Action<Guid, RavenJObject> add, 
			string[] includes,
			HashSet<string> loadedIds)
		{
			Add = add;
			Includes = includes;
			Database = database;
			TransactionInformation = transactionInformation;
			LoadedIds = loadedIds;
		}

		public void AlsoInclude(IEnumerable<string> ids)
		{
			foreach (var id in ids)
			{
				LoadId(id, null);
			}	
		}

		private string[] Includes { get; set; }

		private Action<Guid,RavenJObject> Add { get; set; }

		private DocumentDatabase Database { get; set; }

		private TransactionInformation TransactionInformation { get; set; }

		private HashSet<string> LoadedIds { get; set; }

		private readonly static Regex IncludePrefixRegex = new Regex(@"(\([^\)]+\))$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

		public void Execute(RavenJObject document)
		{
			if (Includes == null)
				return;
			foreach (var include in Includes)
			{
				if (string.IsNullOrEmpty(include))
					continue;

				var path = include;
				string prefix = null;
				var match = IncludePrefixRegex.Match(path);
				if (match.Success && match.Groups.Count >= 2)
				{
					prefix = match.Groups[1].Value;
					path = path.Replace(prefix, "");
					prefix = prefix.Substring(1, prefix.Length - 2);
				}

				foreach (var token in document.SelectTokenWithRavenSyntaxReturningFlatStructure(path))
				{
					ExecuteInternal(token.Item1, prefix);
				}
			}
		}

		private void ExecuteInternal(RavenJToken token, string prefix)
		{
			if (token == null)
				return; // nothing to do

			switch (token.Type)
			{
				case JTokenType.Array:
					foreach (var item in (RavenJArray)token)
					{
						ExecuteInternal(item, prefix);
					}
					break;
				case JTokenType.String:
					LoadId(token.Value<string>(), prefix);
					break;
				case JTokenType.Integer:
					LoadId(token.Value<int>().ToString(CultureInfo.InvariantCulture), prefix);
					break;
				// here we ignore everything else
				// if it ain't a string or array, it is invalid
				// as an id
			}
		}

		private void LoadId(string value, string prefix)
		{
			value = (prefix != null ? prefix + value : value);
			if (LoadedIds.Add(value) == false)
				return;

			var includedDoc = Database.Get(value, TransactionInformation);
			if (includedDoc == null) 
				return;

			Debug.Assert(includedDoc.Etag != null);
			Add(includedDoc.Etag.Value, includedDoc.ToJson());
		}
	}
}
