using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Abstractions.Util;

namespace Raven.Client.UniqueConstraints
{
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
			MemberExpression body;
			if (keySelector.Body is MemberExpression)
			{
				body = ((MemberExpression)keySelector.Body);
			}
			else
			{
				var op = ((UnaryExpression)keySelector.Body).Operand;
				body = ((MemberExpression)op);
			}
			var propertyName = body.Member.Name;

			var uniqueId = "UniqueConstraints/" + typeName.ToLowerInvariant() + "/" + propertyName.ToLowerInvariant() + "/" +
			               Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(value);
			var constraintDoc = session.Include<ConstraintDocument>(x => x.RelatedId).Load(uniqueId);
			if (constraintDoc == null)
				return default(T);

			var id = constraintDoc.RelatedId;
			return string.IsNullOrEmpty(id) ? default(T) : session.Load<T>(id);
		}


		public static UniqueConstraintCheckResult<T> CheckForUniqueConstraints<T>(this IDocumentSession session, T entity)
		{
			var properties = UniqueConstraintsTypeDictionary.GetProperties(typeof (T));
			T[] loadedDocs = null;

			if (properties != null)
			{
				var typeName = session.Advanced.DocumentStore.Conventions.GetTypeTagName(typeof (T));

				var constraintsIds = from property in properties
				                     let propertyValue = property.GetValue(entity, null)
				                     where propertyValue != null
				                     select "UniqueConstraints/" + typeName.ToLowerInvariant() + "/" + property.Name.ToLowerInvariant() + "/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(propertyValue.ToString());

				var constraintDocs = session.Include<ConstraintDocument>(x => x.RelatedId).Load(constraintsIds.ToArray());

				var existingDocsIds = constraintDocs.Where(constraintDoc => constraintDoc != null)
				                                    .Select(constraintDoc => constraintDoc.RelatedId)
				                                    .Where(id => string.IsNullOrEmpty(id) == false)
				                                    .ToArray();

				if (existingDocsIds.Any())
				{
					loadedDocs = session.Load<T>(existingDocsIds);
				}
			}

			return new UniqueConstraintCheckResult<T>(entity, properties, loadedDocs);
		}

		public static Task<T> LoadByUniqueConstraintAsync<T>(this IAsyncDocumentSession session, Expression<Func<T, object>> keySelector, object value)
		{
			if (value == null) throw new ArgumentNullException("value", "The unique value cannot be null");
			var typeName = session.Advanced.DocumentStore.Conventions.GetTypeTagName(typeof (T));
			var body = (MemberExpression) keySelector.Body;
			var propertyName = body.Member.Name;

			var uniqueId = "UniqueConstraints/" + typeName.ToLowerInvariant() + "/" + propertyName.ToLowerInvariant() + "/" +
			               Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(value);

			return session.Include<ConstraintDocument>(x => x.RelatedId).LoadAsync(uniqueId)
			              .ContinueWith(x =>
			              {
				              if (x.Result == null)
					              return new CompletedTask<T>(default(T));
				              return session.LoadAsync<T>(x.Result.RelatedId);
			              }).Unwrap();
		}


		public static Task<UniqueConstraintCheckResult<T>> CheckForUniqueConstraintsAsync<T>(this IAsyncDocumentSession session, T entity)
		{
			var properties = UniqueConstraintsTypeDictionary.GetProperties(typeof (T));

			if (properties != null)
			{
				var typeName = session.Advanced.DocumentStore.Conventions.GetTypeTagName(typeof (T));

				var constraintsIds = from property in properties
				                     let propertyValue = property.GetValue(entity, null)
				                     where propertyValue != null
				                     select "UniqueConstraints/" + typeName.ToLowerInvariant() + "/" + property.Name.ToLowerInvariant() + "/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(propertyValue.ToString());

				return session.Include<ConstraintDocument>(x => x.RelatedId).LoadAsync(constraintsIds.ToArray())
				              .ContinueWith(task =>
				              {
					              var constraintDocs = task.Result;
					              var existingDocsIds = (from constraintDoc in constraintDocs
					                                     where constraintDoc != null
					                                     select constraintDoc.RelatedId
					                                     into id where !string.IsNullOrEmpty(id) select id).ToArray();

					              if (existingDocsIds.Any() == false)
					              {
						              return new CompletedTask<UniqueConstraintCheckResult<T>>(new UniqueConstraintCheckResult<T>(entity, properties, null));
					              }

					              return session.LoadAsync<T>(existingDocsIds.ToArray()).ContinueWith(loadTask =>
					              {
						              var completedTask = new CompletedTask<UniqueConstraintCheckResult<T>>(
							              new UniqueConstraintCheckResult<T>(entity, properties, loadTask.Result)
							              );

						              return (Task<UniqueConstraintCheckResult<T>>) completedTask;
					              }).Unwrap();
				              }).Unwrap();
			}

			return new CompletedTask<UniqueConstraintCheckResult<T>>(new UniqueConstraintCheckResult<T>(entity, properties, null));
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
				var foundDocument = default(T);

				if (loadedDocs != null)
				{
					foundDocument = loadedDocs.FirstOrDefault(x =>
					{
						var docProperty = propertyInfo.GetValue(x, null);
						return docProperty.ToString().Equals(checkProperty.ToString(), StringComparison.InvariantCultureIgnoreCase);
						;
					});
				}
				propertyDocuments.Add(propertyInfo, foundDocument);
			}
		}

		public T[] LoadedDocuments
		{
			get { return loadedDocs; }
		}

		public bool ConstraintsAreFree()
		{
			return propertyDocuments.All(x => x.Value == null);
		}

		public T DocumentForProperty(Expression<Func<T, object>> keySelector)
		{
			var body = (MemberExpression) keySelector.Body;
			var prop = (PropertyInfo) body.Member;
			object doc;
			propertyDocuments.TryGetValue(prop, out doc);
			return (T) doc;
		}
	}
}
