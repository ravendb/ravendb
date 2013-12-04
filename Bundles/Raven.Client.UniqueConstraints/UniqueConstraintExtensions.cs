using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Json.Linq;

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
			var propertyName = GetPropropertyNameForKeySelector<T>(keySelector);

			return LoadByUniqueConstraintInternal<T>(session, propertyName, new object[] { value }).FirstOrDefault();
		}


		public static T[] LoadByUniqueConstraint<T>(this IDocumentSession session, Expression<Func<T, object>> keySelector, params object[] values)
		{
			var propertyName = GetPropropertyNameForKeySelector<T>(keySelector);

			return LoadByUniqueConstraintInternal<T>(session, propertyName, values);
		}


		public static T LoadByUniqueConstraint<T>(this IDocumentSession session, string keyName, object value)
		{
			return LoadByUniqueConstraintInternal<T>(session, keyName, new object[] { value }).FirstOrDefault();
		}


		public static T[] LoadByUniqueConstraint<T>(this IDocumentSession session, string keyName, params object[] values)
		{
			return LoadByUniqueConstraintInternal<T>(session, keyName, values);
		}


		private static T[] LoadByUniqueConstraintInternal<T>(this IDocumentSession session, string propertyName, params object[] values)
		{
			if (values == null) throw new ArgumentNullException("value", "The unique value cannot be null");
			if (string.IsNullOrWhiteSpace(propertyName)) { throw (propertyName == null) ? new ArgumentNullException("propertyName") : new ArgumentException("propertyName cannot be empty.", "propertyName"); }

			if (values.Length == 0) { return new T[0]; }

			var typeName = session.Advanced.DocumentStore.Conventions.GetTypeTagName(typeof(T));
			
			var constraintInfo = session.Advanced.DocumentStore.GetUniquePropertiesForType(typeof(T)).SingleOrDefault(ci => ci.Configuration.Name == propertyName);

			if (constraintInfo != null)
			{
				var uniqueIds =
					(from value in values
					 where value != null
					 select "UniqueConstraints/" + typeName.ToLowerInvariant() + "/" + propertyName.ToLowerInvariant() + "/" +
								Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(value, constraintInfo.Configuration.CaseInsensitive))
					.ToArray();
				var constraintDocs = session.Include("RelatedId").Load<RavenJObject>(uniqueIds);
				if (constraintDocs != null)
				{
					string nullId = Guid.NewGuid().ToString(); // simple way to maintain parallel results array - this ID should never exist in the DB
					var ids = constraintDocs.Select(d => d == null ? nullId : d.Value<string>("RelatedId")).ToArray();
					return session.Load<T>(ids);
				}
			}

			return values.Select(v => default(T)).ToArray();
		}



		private static string GetPropropertyNameForKeySelector<T>(Expression<Func<T, object>> keySelector)
		{
			var body = GetMemberExpression(keySelector);
			var propertyName = body.Member.Name;
			return propertyName;
		}


	    private static MemberExpression GetMemberExpression<T>(Expression<Func<T, object>> keySelector)
	    {
	        MemberExpression body;
	        if (keySelector.Body is MemberExpression)
	        {
	            body = ((MemberExpression) keySelector.Body);
	        }
	        else
	        {
	            var op = ((UnaryExpression) keySelector.Body).Operand;
	            body = ((MemberExpression) op);
	        }
	        return body;
	    }


	    public static UniqueConstraintCheckResult<T> CheckForUniqueConstraints<T>(this IDocumentSession session, T entity)
		{
			var properties = session.Advanced.DocumentStore.GetUniquePropertiesForType(typeof(T));
			T[] loadedDocs = null;
			var typeName = session.Advanced.DocumentStore.Conventions.GetTypeTagName(typeof (T));

			var constraintsIds = from property in properties
			                     let propertyValue = property.GetValue(entity)
			                     where propertyValue != null
								 from item in typeof(IEnumerable).IsAssignableFrom(propertyValue.GetType()) && propertyValue.GetType() != typeof(string) ? ((IEnumerable)propertyValue).Cast<object>().Where(i => i != null) : new[] { propertyValue }
			                     select 
				                     "UniqueConstraints/" + typeName.ToLowerInvariant() + "/" + property.Configuration.Name.ToLowerInvariant() +
				                     "/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(item.ToString(),property.Configuration.CaseInsensitive);

			var constraintDocs = session.Include<ConstraintDocument>(x => x.RelatedId).Load(constraintsIds.ToArray());

			var existingDocsIds = constraintDocs.Where(constraintDoc => constraintDoc != null)
			                                    .Select(constraintDoc => constraintDoc.RelatedId)
			                                    .Where(id => string.IsNullOrEmpty(id) == false)
			                                    .ToArray();

			if (existingDocsIds.Any())
			{
				loadedDocs = session.Load<T>(existingDocsIds);
			}

			return new UniqueConstraintCheckResult<T>(entity, properties, loadedDocs);
		}

		public static Task<T> LoadByUniqueConstraintAsync<T>(this IAsyncDocumentSession session, Expression<Func<T, object>> keySelector, object value)
		{
			if (value == null) throw new ArgumentNullException("value", "The unique value cannot be null");
			var typeName = session.Advanced.DocumentStore.Conventions.GetTypeTagName(typeof (T));
            var body = GetMemberExpression(keySelector);
			var propertyName = body.Member.Name;
		    var att = (UniqueConstraintAttribute) Attribute.GetCustomAttribute(body.Member, typeof (UniqueConstraintAttribute));

		    var uniqueId = "UniqueConstraints/" + typeName.ToLowerInvariant() + "/" + propertyName.ToLowerInvariant() + "/" +
			               Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(value,att.CaseInsensitive);

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
			var properties = session.Advanced.DocumentStore.GetUniquePropertiesForType(typeof(T));

			if (properties != null)
			{
				var typeName = session.Advanced.DocumentStore.Conventions.GetTypeTagName(typeof (T));

				var constraintsIds = from property in properties
				                     let propertyValue = property.GetValue(entity)
				                     where propertyValue != null
				                     select "UniqueConstraints/" + typeName.ToLowerInvariant() + "/" + property.Configuration.Name.ToLowerInvariant() + "/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(propertyValue.ToString(), property.Configuration.CaseInsensitive);

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


		internal static ConstraintInfo[] GetUniquePropertiesForType(this IDocumentStore store, Type type)
		{
			UniqueConstraintsTypeDictionary dictionary = UniqueConstraintsTypeDictionary.FindDictionary(store);
			if (dictionary != null)
			{
				return dictionary.GetProperties(type);
			}

			return null;
		}
	}


	public class UniqueConstraintCheckResult<T>
	{
		private readonly Dictionary<string, object> propertyDocuments;

		private readonly T[] loadedDocs;

		private readonly T checkedDocument;

		public UniqueConstraintCheckResult(T document, ConstraintInfo[] properties, T[] loadedDocs)
		{
			propertyDocuments = new Dictionary<string, object>(properties.Length);
			checkedDocument = document;
			this.loadedDocs = loadedDocs;

			foreach (var propertyInfo in properties)
			{
				var checkProperty = propertyInfo.GetValue(checkedDocument);
				if (checkProperty == null)
					continue;
				var foundDocument = default(T);

				if (loadedDocs != null)
				{
					foundDocument = loadedDocs.FirstOrDefault(x =>
					{
						var docProperty = propertyInfo.GetValue(x);
						if (docProperty == null)
							return false;
						return docProperty.ToString().Equals(checkProperty.ToString(), StringComparison.InvariantCultureIgnoreCase);
					});
				}
				propertyDocuments.Add(propertyInfo.Configuration.Name, foundDocument);
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
			propertyDocuments.TryGetValue(prop.Name, out doc);
			return (T) doc;
		}
	}
}
