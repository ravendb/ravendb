using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

using Raven.Abstractions;
using Raven.Abstractions.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Imports.Newtonsoft.Json.Utilities;
using Raven.Abstractions.Data;
using System.Text;

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
			if (token == null || token.Type == JTokenType.Null)
				return default(U);

			var value = token as RavenJValue;
			if (value == null)
				throw new InvalidCastException("Cannot cast {0} to {1}.".FormatWith(CultureInfo.InvariantCulture, token.GetType(), typeof(U)));

			if (value.Value is U)
				return (U)value.Value;

			Type targetType = typeof(U);

			if (targetType.IsGenericType() && targetType.GetGenericTypeDefinition() == typeof(Nullable<>))
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
			if (targetType == typeof(DateTime))
			{
				var s = value.Value as string;
				if (s != null)
				{
					DateTime dateTime;
					if (DateTime.TryParseExact(s, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
						DateTimeStyles.RoundtripKind, out dateTime))
						return (U) (object) dateTime;

					dateTime = RavenJsonTextReader.ParseDateMicrosoft(s);
					return (U) (object) dateTime;
				}
				if (value.Value is DateTimeOffset)
				{
					return (U)(object)((DateTimeOffset) value.Value).UtcDateTime;
				}
			}
			if (targetType == typeof(DateTimeOffset))
			{
				var s = value.Value as string;
				if (s != null)
				{
					DateTimeOffset dateTimeOffset;
					if (DateTimeOffset.TryParseExact(s, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
						DateTimeStyles.RoundtripKind, out dateTimeOffset))
						return (U) (object) dateTimeOffset;

					return default(U);
				}
				if (value.Value is DateTime)
				{
					return (U) (object) (new DateTimeOffset((DateTime) value.Value));
				}
			}
            if (targetType == typeof(byte[]) && value.Value is string)
            {
                return (U)(object)System.Convert.FromBase64String((string)value.Value);
            }

			if (value.Value == null && typeof(U).IsValueType)
				throw new InvalidOperationException("value.Value == null and conversion target type is not nullable");

			try
			{
				return (U) System.Convert.ChangeType(value.Value, targetType, CultureInfo.InvariantCulture);
			}
			catch (Exception e)
			{
				if (value.Value != null)
					throw new InvalidOperationException(string.Format("Unable to find suitable conversion for {0} since it is not predefined and does not implement IConvertible. ", value.Value.GetType()),e);
				
				throw new InvalidOperationException(string.Format("Unable to find suitable conversion for {0} since it is not predefined ", value),e);
			}
		}

        public static bool CompareRavenJArrayData(this ICollection<DocumentsChanges> docChanges, RavenJArray selfArray, RavenJArray otherArray, string fieldArrName)
        {
            var differences = selfArray.Except(otherArray);

            if (selfArray.Length < otherArray.Length)
            {
                differences = otherArray.Except(selfArray);
            }
            foreach (var dif in differences)
            {
                var changes = new DocumentsChanges
                {
                    FieldName = fieldArrName
                };


                if (selfArray.Length < otherArray.Length)
                {
                    changes.Comment = DocumentsChanges.CommentAsText(DocumentsChanges.CommentType.FieldRemoved);
                    changes.FieldOldValue = dif.ToString();
                    changes.FieldOldType = dif.Type.ToString();
                }

                if (selfArray.Length > otherArray.Length)
                {
                    changes.Comment = DocumentsChanges.CommentAsText(DocumentsChanges.CommentType.FieldAdded);
                    changes.FieldNewValue = dif.ToString();
                    changes.FieldNewType = dif.Type.ToString();
                }
                docChanges.Add(changes);
            }
            return false;
        }

        public static bool CompareDifferentLengthRavenJObjectData(this ICollection<DocumentsChanges> docChanges, RavenJObject otherObj, RavenJObject selfObj, string fieldName)
        {
           
            var diffData = new Dictionary<string, string>();
            var descr = string.Empty;
            RavenJToken token1;
            if (otherObj.Count == 0)
            {
                foreach (var kvp in selfObj.Properties)
                {
                    var changes = new DocumentsChanges();

                    if (selfObj.Properties.TryGetValue(kvp.Key, out token1))
                    {
                        changes.FieldNewValue = token1.ToString();
                        changes.FieldNewType = token1.Type.ToString();
                        changes.Comment = DocumentsChanges.CommentAsText(DocumentsChanges.CommentType.FieldAdded);

                        changes.FieldName = kvp.Key;
                    }

                    changes.FieldOldValue = "null";
                    changes.FieldOldType = "null";

                    docChanges.Add(changes);
                }

               return false;
            }
            CompareJsonData(selfObj.Properties, otherObj.Properties, diffData);

            foreach (var key in diffData.Keys)
            {
                var changes = new DocumentsChanges();

                descr = "field" + fieldName;
                changes.FieldOldType = otherObj.Type.ToString();
                changes.FieldNewType = selfObj.Type.ToString();
                changes.FieldName = key;

                if (selfObj.Count < otherObj.Count)
                {
                    changes.Comment = DocumentsChanges.CommentAsText(DocumentsChanges.CommentType.FieldRemoved);

                    changes.FieldOldValue = diffData[key];
                }

                else
                {
                    changes.Comment = DocumentsChanges.CommentAsText(DocumentsChanges.CommentType.FieldAdded);
                    changes.FieldNewValue = diffData[key];
                }
                docChanges.Add(changes);
            }
            return false;
        }


        private static  void CompareJsonData(DictionaryWithParentSnapshot selfObj, DictionaryWithParentSnapshot otherObj, Dictionary<string, string> diffData)
        {
            RavenJToken token;
            var sb = new StringBuilder();
            var diffNames = selfObj.Keys.Except(otherObj.Keys).ToArray();
            var bigObj = selfObj;
            if (diffData == null)
            {
                diffData = new Dictionary<string, string>();
            }
            if (selfObj.Keys.Count < otherObj.Keys.Count)
            {
                diffNames = otherObj.Keys.Except(selfObj.Keys).ToArray();
                bigObj = otherObj;
            }
            foreach (var kvp in diffNames)
            {
                if (bigObj.TryGetValue(kvp, out token))
                {
                    diffData[kvp] = token.ToString();
                }
            }
        }
        public static bool AddChanges(this List<DocumentsChanges> docChanges, DocumentsChanges.CommentType comment)
        {
            var changes = new DocumentsChanges
            {
                Comment = DocumentsChanges.CommentAsText(comment)
            };
            docChanges.Add(changes);
            return false;
        }
        public static bool AddChanges(this ICollection<DocumentsChanges> docChanges, KeyValuePair<string, RavenJToken> kvp, RavenJToken token)
        {
            var changes = new DocumentsChanges

            {
                FieldNewType = kvp.Value.Type.ToString(),
                FieldOldType = token.Type.ToString(),
                FieldNewValue = kvp.Value.ToString(),
                FieldOldValue = token.ToString(),
                Comment = DocumentsChanges.CommentAsText(DocumentsChanges.CommentType.FieldChanged),
                FieldName = kvp.Key
            };
            docChanges.Add(changes);
            return false;
        }
        public static bool AddChanges(this ICollection<DocumentsChanges> docChanges, RavenJToken curThisReader, RavenJToken curOtherReader, string fieldName)
        {
            var changes = new DocumentsChanges
            {
                FieldNewType = curThisReader.Type.ToString(),
                FieldOldType = curOtherReader.Type.ToString(),
                FieldNewValue = curThisReader.ToString(),
                FieldOldValue = curOtherReader.ToString(),
                Comment = DocumentsChanges.CommentAsText(DocumentsChanges.CommentType.FieldChanged),
                FieldName = fieldName
            };
            docChanges.Add(changes);
            return false;
        }
	}
}
