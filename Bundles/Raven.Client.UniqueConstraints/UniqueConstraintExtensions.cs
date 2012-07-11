using System.Text;

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
		public class ConstraintDocument
		{
			public string RelatedId { get; set; }
		}

		public static T LoadByUniqueConstraint<T>(this IDocumentSession session, Expression<Func<T, object>> keySelector, object value)
		{
			if (value == null) throw new ArgumentNullException("value", "The unique value cannot be null");
			var typeName = session.Advanced.DocumentStore.Conventions.GetTypeTagName(typeof(T));
			var body = (MemberExpression)keySelector.Body;
			var propertyName = body.Member.Name;


			string uniqueId = "UniqueConstraints/" + typeName.ToLowerInvariant() + "/" + propertyName.ToLowerInvariant() + "/" + 
				Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(value);
			var constraintDoc = session.Include("Id").Load<ConstraintDocument>(uniqueId);
			if (constraintDoc == null)
				return default(T);

			var id = constraintDoc.RelatedId;
					
			if (!string.IsNullOrEmpty(id))
			{
				return session.Load<T>(id);
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
					var propertyValue = property.GetValue(entity, null);
					if(propertyValue == null)
						continue;
					constraintsIds.Add("UniqueConstraints/" + typeName.ToLowerInvariant() + "/" + property.Name.ToLowerInvariant() + "/" + 
						Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(propertyValue.ToString()));
				}

				ConstraintDocument[] constraintDocs = session.Include("Id").Load<ConstraintDocument>(constraintsIds.ToArray());

				foreach (var constraintDoc in constraintDocs)
				{
					if(constraintDoc == null)
						continue;

					var id = constraintDoc.RelatedId;
					if (!string.IsNullOrEmpty(id))
					{
						existingDocsIds.Add(id);
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
		private readonly Dictionary<PropertyInfo, object> propertyDocuments;

		private readonly T[] loadedDocs;

		private readonly T checkedDocument;

		public UniqueConstraintCheckResult(T document, PropertyInfo[] properties, T[] loadedDocs)
		{
			propertyDocuments = new Dictionary<PropertyInfo, object>(properties.Count());
			checkedDocument = document;
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
						var docProperty = propertyInfo.GetValue(x, null);
						return docProperty.ToString().Equals(checkProperty.ToString(), StringComparison.InvariantCultureIgnoreCase); ;
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
			object doc = null;

			propertyDocuments.TryGetValue(prop, out doc);

			return (T)doc;
		}
	}
}
