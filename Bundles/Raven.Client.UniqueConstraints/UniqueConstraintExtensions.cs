namespace Raven.Client.UniqueConstraints
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Linq.Expressions;
	using System.Reflection;

	using Raven.Json.Linq;

	public static class UniqueConstraintExtensions
	{
		public static T LoadByUniqueConstraint<T>(this IDocumentSession session, Expression<Func<T, object>> keySelector, object value)
		{
			var typeName = session.Advanced.DocumentStore.Conventions.GetTypeTagName(typeof(T));
			var body = (MemberExpression)keySelector.Body;
			var propertyName = body.Member.Name;

			var constraintDoc = session.Include("Id").Load<dynamic>("UniqueConstraints/" + typeName.ToLowerInvariant() + "/" + propertyName.ToLowerInvariant() + "/" + value);

			if (constraintDoc != null && !string.IsNullOrEmpty(constraintDoc.Id))
			{
				return session.Load<T>(constraintDoc.Id.ToString());
			}

			return default(T);
		}

		public static UniqueConstraintCheckResult<T> CheckForUniqueConstraints<T>(this IDocumentSession session, T entity)
		{
			var properties = UniqueConstraintsTypeDictionary.GetProperties(typeof(T));
			T[] loadedDocs = null;

			if (properties != null)
			{
				var typeName = session.Advanced.DocumentStore.Conventions.GetTypeTagName(typeof(T));
				var constraintsIds = new List<string>();
				var existingDocsIds = new List<string>();

				foreach (var property in properties)
				{
					var propertyValue = property.GetValue(entity, null).ToString();
					constraintsIds.Add("UniqueConstraints/" + typeName.ToLowerInvariant() + "/" + property.Name.ToLowerInvariant() + "/" + propertyValue);
				}

				dynamic[] constraintDocs = session.Include("Id").Load<dynamic>(constraintsIds.ToArray());

				foreach (var constraintDoc in constraintDocs)
				{
					if (constraintDoc != null && !string.IsNullOrEmpty(constraintDoc.Id) )
					{
						existingDocsIds.Add(constraintDoc.Id);
					}
				}

				if (existingDocsIds.Count > 0 )
				{
					loadedDocs = session.Load<T>(existingDocsIds.ToArray());
				}
			}

			return new UniqueConstraintCheckResult<T>(entity, properties, loadedDocs);
		}
	}

	public class UniqueConstraintCheckResult<T>
	{
		private readonly Dictionary<PropertyInfo, dynamic> propertyDocuments;

		private readonly T[] loadedDocs;

		private readonly T checkedDocument;

		public UniqueConstraintCheckResult(T document, PropertyInfo[] properties, T[] loadedDocs)
		{
			this.propertyDocuments = new Dictionary<PropertyInfo, dynamic>(properties.Count());
			this.checkedDocument = document;
			this.loadedDocs = loadedDocs;

			if (properties == null)
			{
				properties = new PropertyInfo[0];
			}

			foreach (var propertyInfo in properties)
			{
				var checkProperty = propertyInfo.GetValue(checkedDocument, null);
				T foundDocument = default(T);

				if (loadedDocs != null)
				{
					foundDocument = loadedDocs.FirstOrDefault(x =>
					{
						var docProperty = propertyInfo.GetValue((T)x, null);
						return docProperty.ToString().Equals(checkProperty); ;
					});
				}
				propertyDocuments.Add(propertyInfo, foundDocument);
				
			}
		}

		public T[] LoadedDocuments
		{
			get
			{
				return this.loadedDocs;
			}
		}

		public bool ConstraintsAreFree()
		{
			return propertyDocuments.All(x => x.Value == null);
		}

		public T DocumentForProperty(Expression<Func<T, object>> keySelector)
		{
			var body = (MemberExpression)keySelector.Body;
			var prop = (PropertyInfo)body.Member;
			dynamic doc = null;

			propertyDocuments.TryGetValue(prop, out doc);

			return doc;
		}
	}
}
