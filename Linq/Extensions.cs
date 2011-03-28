using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Json.Utilities;

namespace Raven.Json.Linq
{
	public static class Extensions
	{
		/// <summary>
		/// Converts the value.
		/// </summary>
		/// <typeparam name="U">The type to convert the value to.</typeparam>
		/// <param name="value">A <see cref="RavenJToken"/> cast as a <see cref="IEnumerable{T}"/> of <see cref="RavenJToken"/>.</param>
		/// <returns>A converted value.</returns>
		public static U Value<U>(this IEnumerable<RavenJToken> value)
		{
			return value.Value<RavenJToken, U>();
		}

		public static U Value<U>(this RavenJToken value)
		{
			return value.Convert<RavenJToken, U>();
		}

		/// <summary>
		/// Converts the value.
		/// </summary>
		/// <typeparam name="T">The source collection type.</typeparam>
		/// <typeparam name="U">The type to convert the value to.</typeparam>
		/// <param name="value">A <see cref="RavenJToken"/> cast as a <see cref="IEnumerable{T}"/> of <see cref="RavenJToken"/>.</param>
		/// <returns>A converted value.</returns>
		public static U Value<T, U>(this IEnumerable<T> value) where T : RavenJToken
		{
			ValidationUtils.ArgumentNotNull(value, "source");

			var token = value as RavenJToken;
			if (token == null)
				throw new ArgumentException("Source value must be a JToken.");

			return token.Convert<RavenJToken, U>();
		}

		internal static U Convert<T, U>(this T token) where T : RavenJToken
		{
			bool cast = typeof(RavenJToken).IsAssignableFrom(typeof(U));

			return Convert<T, U>(token, cast);
		}

		internal static U Convert<T, U>(this T token, bool cast) where T : RavenJToken
		{
			if (cast)
			{
				// HACK
				return (U)(object)token;
			}
			else
			{
				if (token == null)
					return default(U);

				var value = token as RavenJValue;
				if (value == null)
					throw new InvalidCastException("Cannot cast {0} to {1}.".FormatWith(CultureInfo.InvariantCulture, token.GetType(), typeof(T)));

				if (value.Value is U)
					return (U)value.Value;

				Type targetType = typeof(U);

				if (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(Nullable<>))
				{
					if (value.Value == null)
						return default(U);

					targetType = Nullable.GetUnderlyingType(targetType);
				}

				return (U)System.Convert.ChangeType(value.Value, targetType, CultureInfo.InvariantCulture);
			}
		}
	}
}
