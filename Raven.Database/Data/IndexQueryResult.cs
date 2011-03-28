//-----------------------------------------------------------------------
// <copyright file="IndexQueryResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Newtonsoft.Json.Linq;

namespace Raven.Database.Data
{
	public class IndexQueryResult
	{
		public string Key { get; set; }
		public JObject Projection { get; set; }

		public bool Equals(IndexQueryResult other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return Equals(other.Key, Key) && new JTokenEqualityComparer().Equals(other.Projection, Projection);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			return Equals((IndexQueryResult) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				return ((Key != null ? Key.GetHashCode() : 0)*397) ^ (Projection != null ? new JTokenEqualityComparer().GetHashCode(Projection) : 0);
			}
		}
	}
}
