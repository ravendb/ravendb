using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Raven.Client.Util
{

	/// <summary>
	/// A generic object comparerer that would only use object's reference, 
	/// ignoring any <see cref="IEquatable{T}"/> or <see cref="object.Equals(object)"/>  overrides.
	/// </summary>
	public class ObjectReferenceEqualityComparerer<T> : EqualityComparer<T>
		where T : class
	{
		/// <summary>
		/// The default ObjectReferenceEqualityComparerer instance
		/// </summary>
		public new static readonly IEqualityComparer<T> Default = new ObjectReferenceEqualityComparerer<T>();

		/// <summary>
		/// When overridden in a derived class, determines whether two objects of type <typeparamref name="T"/> are equal.
		/// </summary>
		/// <returns>
		/// true if the specified objects are equal; otherwise, false.
		/// </returns>
		/// <param name="x">The first object to compare.</param><param name="y">The second object to compare.</param>
		public override bool Equals(T x, T y)
		{
			return object.ReferenceEquals(x, y);
		}

		/// <summary>
		/// When overridden in a derived class, serves as a hash function for the specified object for hashing algorithms and data structures, such as a hash table.
		/// </summary>
		/// <returns>
		/// A hash code for the specified object.
		/// </returns>
		/// <param name="obj">The object for which to get a hash code.</param><exception cref="T:System.ArgumentNullException">The type of <paramref name="obj"/> is a reference type and <paramref name="obj"/> is null.</exception>
		public override int GetHashCode(T obj)
		{
			return RuntimeHelpers.GetHashCode(obj);
		}
	}
}