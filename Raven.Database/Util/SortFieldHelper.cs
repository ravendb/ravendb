// -----------------------------------------------------------------------
//  <copyright file="IndexFieldHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Util
{
	public static class SortFieldHelper
    {
		public static Field CustomField(string field)
		{
			var parts = field.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
			return parts.Length == 1 ? new Field(parts[0]) : new Field(parts[0], parts[1]);
		}

		public class Field
		{
			public Field(string type)
			{
				Type = type;
			}

			public Field(string type, string name)
			{
				Type = type;
				Name = name;
			}

			public string Type { get; private set; }
			public string Name { get; private set; }
		}
    }
}