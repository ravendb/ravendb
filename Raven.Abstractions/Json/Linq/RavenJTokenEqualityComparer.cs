using System.Collections.Generic;

namespace Raven.Json.Linq
{
	/// <summary>
	/// Compares tokens to determine whether they are equal.
	/// </summary>
	public class RavenJTokenEqualityComparer : IEqualityComparer<RavenJToken>, IEqualityComparer<object>
	{
		public readonly static RavenJTokenEqualityComparer Default = new RavenJTokenEqualityComparer();

		/// <summary>
		/// Determines whether the specified objects are equal.
		/// </summary>
		/// <param name="x">The first object of type <see cref="RavenJToken"/> to compare.</param>
		/// <param name="y">The second object of type <see cref="RavenJToken"/> to compare.</param>
		/// <returns>
		/// true if the specified objects are equal; otherwise, false.
		/// </returns>
		public bool Equals(RavenJToken x, RavenJToken y)
		{
			return RavenJToken.DeepEquals(x, y);
		}

		/// <summary>
		/// Returns a hash code for the specified object.
		/// </summary>
		/// <param name="obj">The <see cref="T:System.Object"/> for which a hash code is to be returned.</param>
		/// <returns>A hash code for the specified object.</returns>
		/// <exception cref="T:System.ArgumentNullException">The type of <paramref name="obj"/> is a reference type and <paramref name="obj"/> is null.</exception>
		public int GetHashCode(RavenJToken obj)
		{
			if (obj == null)
				return 0;

			return obj.GetDeepHashCode();
		}

		public new bool Equals(object x, object y)
		{
			return this.Equals((RavenJToken) x, (RavenJToken) y);
		}

		public int GetHashCode(object obj)
		{
			return GetHashCode((RavenJToken) obj);
		}
	}
}
