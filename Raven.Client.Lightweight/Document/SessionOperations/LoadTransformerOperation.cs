using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Document.SessionOperations
{
	public class LoadTransformerOperation
	{
        private readonly InMemoryDocumentSessionOperations documentSession;
		private readonly string transformer;
		private readonly string[] ids;

		public LoadTransformerOperation(InMemoryDocumentSessionOperations documentSession, string transformer, string[] ids)
		{
			this.documentSession = documentSession;
			this.transformer = transformer;
			this.ids = ids;
		}

		public T[] Complete<T>(MultiLoadResult multiLoadResult)
		{
			if (typeof (T).IsArray)
			{
			    var arrayOfArrays = multiLoadResult.Results
			                                       .Select(x =>
					{
			                                           if (x == null)
			                                               return null;

			                                           var values = x.Value<RavenJArray>("$values").Cast<RavenJObject>();

						var elementType = typeof (T).GetElementType();
			                                           var array = values.Select(value =>
						{
                                                           EnsureNotReadVetoed(value);
			                                               return documentSession.ProjectionToInstance(value, elementType);
						}).ToArray();
						var newArray = Array.CreateInstance(elementType, array.Length);
						Array.Copy(array, newArray, array.Length);
						return newArray;
					})
					.Cast<T>()
					.ToArray();

				return arrayOfArrays;
			}

			var items = ParseResults<T>(multiLoadResult.Results)
				.ToArray();

			if (items.Length > ids.Length)
			{
				throw new InvalidOperationException(String.Format("A load was attempted with transformer {0}, and more than one item was returned per entity - please use {1}[] as the projection type instead of {1}",
					transformer,
					typeof (T).Name));
			}

			return items;
		}

		private IEnumerable<T> ParseResults<T>(List<RavenJObject> results)
		{
			foreach (var result in results)
			{
				if (result == null)
				{
					yield return default(T);
					continue;
				}

			    EnsureNotReadVetoed(result);

				var values = result.Value<RavenJArray>("$values").ToArray();
				foreach (var value in values)
				{
					var ravenJObject = JsonExtensions.ToJObject(value);
					var obj = (T) documentSession.ProjectionToInstance(ravenJObject, typeof (T));
					yield return obj;
				}
			}
		}

	    private bool EnsureNotReadVetoed(RavenJObject result)
	    {
            var metadata = result.Value<RavenJObject>(Constants.Metadata);
            if (metadata != null)
                documentSession.EnsureNotReadVetoed(metadata); // this will throw on read veto

	        return true;
	    }
	}
}