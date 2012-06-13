using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Json;
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
			return value.Convert<U>();
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
			var token = value as RavenJToken;
			if (token == null)
				throw new ArgumentException("Source value must be a RavenJToken.");

			return token.Convert<U>();
		}

		/// <summary>
		/// Returns a collection of converted child values of every object in the source collection.
		/// </summary>
		/// <typeparam name="U">The type to convert the values to.</typeparam>
		/// <param name="source">An <see cref="IEnumerable{T}"/> of <see cref="RavenJToken"/> that contains the source collection.</param>
		/// <returns>An <see cref="IEnumerable{T}"/> that contains the converted values of every node in the source collection.</returns>
		public static IEnumerable<U> Values<U>(this IEnumerable<RavenJToken> source)
		{
			return Values<U>(source, null);
		}

		/// <summary>
		/// Returns a collection of child values of every object in the source collection with the given key.
		/// </summary>
		/// <param name="source">An <see cref="IEnumerable{T}"/> of <see cref="RavenJToken"/> that contains the source collection.</param>
		/// <param name="key">The token key.</param>
		/// <returns>An <see cref="IEnumerable{T}"/> of <see cref="RavenJToken"/> that contains the values of every node in the source collection with the given key.</returns>
		public static IEnumerable<RavenJToken> Values(this IEnumerable<RavenJToken> source, string key)
		{
			return Values<RavenJToken>(source, key);
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

		internal static IEnumerable<U> Values<U>(this IEnumerable<RavenJToken> source, string key)
		{
			foreach (RavenJToken token in source)
			{
				if (token is RavenJValue)
				{
					yield return Convert<U>(token);
				}
				else
				{
					foreach (var t in token.Values<U>())
					{
						yield return t;
					}
				}

				var ravenJObject = (RavenJObject) token;

				RavenJToken value = ravenJObject[key];
				if (value != null)
					yield return value.Convert<U>();
			}

			yield break;
		}

		internal static U Convert<U>(this RavenJToken token)
		{
			if (token is RavenJArray && typeof(U) == typeof(RavenJObject))
			{
				var ar = (RavenJArray)token;
				var o = new RavenJObject();
				foreach (RavenJObject item in ar)
				{
					o[item["Key"].Value<string>()] = item["Value"];
				}
				return (U) (object) o;
			}

			bool cast = typeof(RavenJToken).IsAssignableFrom(typeof(U));

			return Convert<U>(token, cast);
		}

		internal static IEnumerable<U> Convert<U>(this IEnumerable<RavenJToken> source)
		{
			bool cast = typeof(RavenJToken).IsAssignableFrom(typeof(U));

			return source.Select(token => Convert<U>(token, cast));
		}

		internal static U Convert<U>(this RavenJToken token, bool cast)
		{
			if (cast)
			{
				// HACK
				return (U)(object)token;
			}
			if (token == null)
				return default(U);

			var value = token as RavenJValue;
			if (value == null)
				throw new InvalidCastException("Cannot cast {0} to {1}.".FormatWith(CultureInfo.InvariantCulture, token.GetType(), typeof(U)));

			if (value.Value is U)
				return (U)value.Value;

			Type targetType = typeof(U);

			if (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(Nullable<>))
			{
				if (value.Value == null)
					return default(U);

				targetType = Nullable.GetUnderlyingType(targetType);
			}
			if(targetType == typeof(Guid))
			{
				if (value.Value == null)
					return default(U);
				return (U)(object)new Guid(value.Value.ToString());
			}
			if (targetType == typeof(string))
			{
				if (value.Value == null)
					return default(U);
				return (U)(object)value.Value.ToString();
			}
			if (targetType == typeof(DateTime) && value.Value is string)
			{
				DateTime dateTime;
				if (DateTime.TryParseExact((string)value.Value, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dateTime))
					return (U)(object)dateTime;
				
				dateTime = RavenJsonTextReader.ParseDateMicrosoft((string)value.Value);
				return (U)(object)dateTime;
			}
			return (U)System.Convert.ChangeType(value.Value, targetType, CultureInfo.InvariantCulture);
		}
	}
}
