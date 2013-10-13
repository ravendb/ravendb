#if !SILVERLIGHT
using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Document.SessionOperations
{
	public class LoadTransformerOperation 
	{
		private readonly DocumentSession documentSession;
		private readonly string transformer;
		private readonly int count;

		public LoadTransformerOperation(DocumentSession documentSession, string transformer, int count)
		{
			this.documentSession = documentSession;
			this.transformer = transformer;
			this.count = count;
		}

		public T[] Complete<T>(MultiLoadResult multiLoadResult)
		{
			if (typeof(T).IsArray)
			{
				// Returns array of arrays, public APIs don't surface that yet though as we only support Transform
				// With a single Id
				var arrayOfArrays = multiLoadResult
					.Results
					.Select(x => x.Value<RavenJArray>("$values").Cast<RavenJObject>())
					.Select(values =>
					{
						var elementType = typeof(T).GetElementType();
						var array = values.Select(y =>
						{
							return documentSession.ProjectionToInstance(y, elementType);
						}).ToArray();
						var newArray = Array.CreateInstance(elementType, array.Length);
						Array.Copy(array, newArray, array.Length);
						return newArray;
					})
					.Cast<T>()
					.ToArray();

				return arrayOfArrays;
			}
			var items = multiLoadResult
				.Results
				.Where(x => x != null)
				.SelectMany(x => x.Value<RavenJArray>("$values").ToArray())
				.Select(JsonExtensions.ToJObject)
				.Select(x =>
				{
					return documentSession.ProjectionToInstance(x, typeof(T));
				})
				.Cast<T>()
				.ToArray();

			if (items.Length > count)
			{
				throw new InvalidOperationException(String.Format("A load was attempted with transformer {0}, and more than one item was returned per entity - please use {1}[] as the projection type instead of {1}",
				                                                  transformer,
				                                                  typeof(T).Name));
			}
			return items;
		}
	}
}
#endif