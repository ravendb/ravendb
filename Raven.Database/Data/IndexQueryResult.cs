//-----------------------------------------------------------------------
// <copyright file="IndexQueryResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Json.Linq;

namespace Raven.Database.Data
{
	public class IndexQueryResult : IEquatable<IndexQueryResult>
	{
		public string Key { get; set; }
		public RavenJObject Projection { get; set; }

		public float Score { get; set; }

		public bool Equals(IndexQueryResult other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return Equals(other.Key, Key) && Equals(other.Projection, Projection) && other.Score.Equals(Score);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != typeof (IndexQueryResult)) return false;
			return Equals((IndexQueryResult) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				int result = (Key != null ? Key.GetHashCode() : 0);
				result = (result*397) ^ (Projection != null ? Projection.GetHashCode() : 0);
				result = (result*397) ^ Score.GetHashCode();
				return result;
			}
		}

		public static bool operator ==(IndexQueryResult left, IndexQueryResult right)
		{
			return Equals(left, right);
		}

		public static bool operator !=(IndexQueryResult left, IndexQueryResult right)
		{
			return !Equals(left, right);
		}
	}
}
