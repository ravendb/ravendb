//-----------------------------------------------------------------------
// <copyright file="ITypeConverter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
namespace Raven.Client.Converters
{
	/// <summary>
	/// Interface for performing type conversions.
	/// We couldn't use the built-in TypeConverter because it is too big an interface for people to build on.
	/// </summary>
	public interface ITypeConverter
	{
		/// <summary>
		/// Returns whether this converter can convert an object of the given type to the type of this converter.
		/// </summary>
		/// <returns>
		/// true if this converter can perform the conversion; otherwise, false.
		/// </returns>
		/// <param name="sourceType">A <see cref="T:System.Type"/> that represents the type you want to convert from.</param>
		bool CanConvertFrom(Type sourceType);
		/// <summary>
		/// Converts the given object to the type of this converter.
		/// </summary>
		/// <returns>
		/// An <see cref="T:System.Object"/> that represents the converted value.
		/// </returns>
		/// <param name="tag">The tag prefix to use</param>
		/// <param name="value">The <see cref="T:System.Object"/> to convert. </param>
		/// <param name="allowNull">Whatever null is a valid value</param>
		/// <exception cref="T:System.NotSupportedException">The conversion cannot be performed. </exception>
		string ConvertFrom(string tag, object value, bool allowNull);
		/// <summary>
		/// Converts the given value object to the specified type, using the specified context and culture information.
		/// </summary>
		/// <returns>
		/// An <see cref="T:System.Object"/> that represents the converted value.
		/// </returns>
		/// <param name="value">The <see cref="T:System.Object"/> to convert. </param>
		object ConvertTo(string value);
	}
}
