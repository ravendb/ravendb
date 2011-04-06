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
				throw new ArgumentException("Source value must be a RavenJToken.");

			return token.Convert<RavenJToken, U>();
		}

		/// <summary>
		/// Returns a collection of converted child values of every object in the source collection.
		/// </summary>
		/// <typeparam name="U">The type to convert the values to.</typeparam>
		/// <param name="source">An <see cref="IEnumerable{T}"/> of <see cref="RavenJToken"/> that contains the source collection.</param>
		/// <returns>An <see cref="IEnumerable{T}"/> that contains the converted values of every node in the source collection.</returns>
		public static IEnumerable<U> Values<U>(this IEnumerable<RavenJToken> source)
		{
			return Values<RavenJToken, U>(source, null);
		}

		/// <summary>
		/// Returns a collection of child values of every object in the source collection with the given key.
		/// </summary>
		/// <param name="source">An <see cref="IEnumerable{T}"/> of <see cref="RavenJToken"/> that contains the source collection.</param>
		/// <param name="key">The token key.</param>
		/// <returns>An <see cref="IEnumerable{T}"/> of <see cref="RavenJToken"/> that contains the values of every node in the source collection with the given key.</returns>
		public static IEnumerable<RavenJToken> Values(this IEnumerable<RavenJToken> source, object key)
		{
			return Values<RavenJToken, RavenJToken>(source, key);
		}

		/// <summary>
		/// Returns a collection of child values of every object in the source collection.
		/// </summary>
		/// <param name="source">An <see cref="IEnumerable{T}"/> of <see cref="RavenJToken"/> that contains the source collection.</param>
		/// <returns>An <see cref="IEnumerable{T}"/> of <see cref="RavenJToken"/> that contains the values of every node in the source collection.</returns>
		public static IEnumerable<RavenJToken> Values(this IEnumerable<RavenJToken> source)
		{
			return source.Values(null);
		}

		internal static IEnumerable<U> Values<T, U>(this IEnumerable<T> source, object key) where T : RavenJToken
		{
			ValidationUtils.ArgumentNotNull(source, "source");

			foreach (RavenJToken token in source)
			{
				if (key == null)
				{
					if (token is RavenJValue)
					{
						yield return Convert<RavenJValue, U>((RavenJValue) token);
					}
					else
					{
						foreach (RavenJToken t in token.Children())
						{
							yield return t.Convert<RavenJToken, U>();
						}
					}
				}
				else
				{
					RavenJToken value = token[key];
					if (value != null)
						yield return value.Convert<RavenJToken, U>();
				}
			}

			yield break;
		}

		internal static U Convert<T, U>(this T token) where T : RavenJToken
		{
			if (token is RavenJArray && typeof(U) == typeof(RavenJObject))
			{
				var ar = token as RavenJArray;
				var o = new RavenJObject();
				foreach (var item in ar.Items)
				{
					o.Properties.Add(item["Key"].Value<string>(), item["Value"]);
				}
				return (U) (object) o;
			}

			bool cast = typeof(RavenJToken).IsAssignableFrom(typeof(U));

			return Convert<T, U>(token, cast);
		}

		internal static IEnumerable<U> Convert<T, U>(this IEnumerable<T> source) where T : RavenJToken
		{
			ValidationUtils.ArgumentNotNull(source, "source");

			bool cast = typeof(RavenJToken).IsAssignableFrom(typeof(U));

			foreach (RavenJToken token in source)
			{
				yield return Convert<RavenJToken, U>(token, cast);
			}
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
