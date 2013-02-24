//-----------------------------------------------------------------------
// <copyright file="SuggestionsHelper.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;

namespace Raven.Tests.Suggestions
{
	public static class SuggestionsHelper
	{
		public class Person
		{
			public string Name { get; set; }
		}

		public static string IndexName { get { return "PersonsByName"; } }

		public static List<Person> GetPersons()
		{
			var names = File.ReadAllLines("./suggestions/names.txt");
			return names.Select(name => new Person {Name = name}).ToList();
		}

		public static IndexDefinition GetIndex(DocumentStore doc)
		{
			return new IndexDefinitionBuilder<Person>()
					   {
						   Map = persons => from p in persons select new {p.Name}
					   }.ToIndexDefinition(doc.Conventions);
			
		}

		public static SuggestionQuery GetQuery(string term, StringDistanceTypes stringDistanceTypes)
		{
			return new SuggestionQuery
					   {
						   Distance = stringDistanceTypes,
						   Field = "Name",
						   MaxSuggestions = 10,
						   Term = term
					   };
		}
	}
}
