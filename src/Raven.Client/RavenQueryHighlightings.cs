// -----------------------------------------------------------------------
//  <copyright file="RavenQueryHighlightings.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Client.Data.Queries;

namespace Raven.Client
{
    public class RavenQueryHighlightings
    {
        private readonly List<FieldHighlightings> _fields = new List<FieldHighlightings>();

        internal FieldHighlightings AddField(string fieldName)
        {
            var fieldHighlightings = new FieldHighlightings(fieldName);
            _fields.Add(fieldHighlightings);
            return fieldHighlightings;
        }

        internal void Update(QueryResult queryResult)
        {
            foreach (var fieldHighlightings in _fields)
                fieldHighlightings.Update(queryResult);
        }
    }
}
