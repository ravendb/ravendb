// -----------------------------------------------------------------------
//  <copyright file="RavenQueryHighlightings.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Client
{
	public class RavenQueryHighlightings
	{
		private readonly List<FieldHighlightings> fields = new List<FieldHighlightings>();

		internal FieldHighlightings AddField(string fieldName)
		{
			var fieldHighlightings = new FieldHighlightings(fieldName);
			this.fields.Add(fieldHighlightings);
			return fieldHighlightings;
		}

		internal void Update(QueryResult queryResult)
		{
			foreach (var fieldHighlightings in this.fields)
				fieldHighlightings.Update(queryResult);
		}
	}
}