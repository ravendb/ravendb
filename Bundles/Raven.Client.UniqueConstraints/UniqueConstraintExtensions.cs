using System;
using System.Collections;
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
			public ConstraintDocument()
			{
				Constraints = new Dictionary<string, Inner>();
			}

			public string RelatedId { get; set; }

			public Dictionary<string, Inner> Constraints { get; set; }

			public string GetRelatedIdFor(string key)
			{
				if (!string.IsNullOrEmpty(RelatedId))
					return RelatedId;

				Inner inner;
				if (Constraints.TryGetValue(key, out inner))
					return inner.RelatedId;

				return null;
			}

			public class Inner
			{
				public string RelatedId { get; set; }
			}
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

		public const string DummyId = "E1972AA7-148D-4035-9779-0EDFB3A7DBFF";
		private static T[] LoadByUniqueConstraintInternal<T>(this IDocumentSession session, string propertyName, params object[] values)
		{
			if (values == null) throw new ArgumentNullException("value", "The unique value cannot be null");
			if (string.IsNullOrWhiteSpace(propertyName)) { throw (propertyName == null) ? new ArgumentNullException("propertyName") : new ArgumentException("propertyName cannot be empty.", "propertyName"); }

			if (values.Length == 0) { return new T[0]; }

			var typeName = session.Advanced.DocumentStore.Conventions.GetTypeTagName(typeof(T));

			var constraintInfo = session.Advanced.DocumentStore.GetUniquePropertiesForType(typeof(T)).SingleOrDefault(ci => ci.Configuration.Name == propertyName);

			if (constraintInfo != null)
			{
				var constraintsIds = (from value in values
									  where value != null
									  select
										  new
										  {
											  Id = "UniqueConstraints/" + typeName.ToLowerInvariant() + "/" + propertyName.ToLowerInvariant() + "/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(value, constraintInfo.Configuration.CaseInsensitive),
											  Key = Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(value, constraintInfo.Configuration.CaseInsensitive)
										  }).ToList();


				var constraintDocs = session
					.Include<ConstraintDocument>(x => x.RelatedId)
					.Include("Constraints.$Values,RelatedId(ConstraintDocuments/)")
					.Load(constraintsIds.Select(x => x.Id).ToArray());

				var existingDocsIds = new List<string>();
				for (var i = 0; i < constraintDocs.Length; i++)
				{
					// simple way to maintain parallel results array - DummyId should never exist in the DB
					var constraintDoc = constraintDocs[i];
					if (constraintDoc == null)
					{
						existingDocsIds.Add(DummyId);
						continue;
					}

					session.Advanced.Evict(constraintDoc);

					var constraintId = constraintsIds[i];
					var relatedId = constraintDoc.GetRelatedIdFor(constraintId.Key);

					existingDocsIds.Add(!string.IsNullOrEmpty(relatedId) ? relatedId : DummyId);
				}

				return session.Load<T>(existingDocsIds);
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
				body = ((MemberExpression)keySelector.Body);
			}
			else
			{
				var op = ((UnaryExpression)keySelector.Body).Operand;
				body = ((MemberExpression)op);
			}
			return body;
		}


		public static UniqueConstraintCheckResult<T> CheckForUniqueConstraints<T>(this IDocumentSession session, T entity)
		{
			var properties = session.Advanced.DocumentStore.GetUniquePropertiesForType(typeof(T));
			T[] loadedDocs = null;
			var typeName = session.Advanced.DocumentStore.Conventions.GetTypeTagName(typeof(T));

			var constraintsIds = (from property in properties
								  let propertyValue = property.GetValue(entity)
								  where propertyValue != null
								  from item in propertyValue is IEnumerable && propertyValue.GetType() != typeof(string) ? ((IEnumerable)propertyValue).Cast<object>().Where(i => i != null) : new[] { propertyValue }
								  select new
								  {
									  Id = "UniqueConstraints/" + typeName.ToLowerInvariant() + "/" + property.Configuration.Name.ToLowerInvariant() + "/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(item.ToString(), property.Configuration.CaseInsensitive),
									  Key = Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(item.ToString(), property.Configuration.CaseInsensitive)
								  }).ToList();

			var constraintDocs = session
				.Include<ConstraintDocument>(x => x.RelatedId)
				.Include("Constraints.$Values,RelatedId(ConstraintDocuments/)")
				.Load(constraintsIds.Select(x => x.Id).ToArray());

			var existingDocsIds = new List<string>();
			for (var i = 0; i < constraintDocs.Length; i++)
			{
				var constraintDoc = constraintDocs[i];
				if (constraintDoc == null)
					continue;

				var constraintId = constraintsIds[i];
				var relatedId = constraintDoc.GetRelatedIdFor(constraintId.Key);

				if (!string.IsNullOrEmpty(relatedId))
					existingDocsIds.Add(relatedId);
			}

			if (existingDocsIds.Any())
			{
				loadedDocs = session.Load<T>(existingDocsIds);
			}

			return new UniqueConstraintCheckResult<T>(entity, properties, loadedDocs);
		}

		public static Task<T> LoadByUniqueConstraintAsync<T>(this IAsyncDocumentSession session, Expression<Func<T, object>> keySelector, object value)
		{
			if (value == null) throw new ArgumentNullException("value", "The unique value cannot be null");
			var typeName = session.Advanced.DocumentStore.Conventions.GetTypeTagName(typeof(T));
			var body = GetMemberExpression(keySelector);
			var propertyName = body.Member.Name;
			var att = (UniqueConstraintAttribute)Attribute.GetCustomAttribute(body.Member, typeof(UniqueConstraintAttribute));

			var escapedValue = Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(value, att.CaseInsensitive);
			var uniqueId = "UniqueConstraints/" + typeName.ToLowerInvariant() + "/" + propertyName.ToLowerInvariant() + "/" + escapedValue;

			return session
				.Include<ConstraintDocument>(x => x.RelatedId)
				.Include("Constraints.$Values,RelatedId(ConstraintDocuments/)")
				.LoadAsync(uniqueId)
						  .ContinueWith(x =>
						  {
							  if (x.Result == null)
								  return new CompletedTask<T>(default(T));
							  return session.LoadAsync<T>(x.Result.GetRelatedIdFor(escapedValue));
						  }).Unwrap();
		}


		public static Task<UniqueConstraintCheckResult<T>> CheckForUniqueConstraintsAsync<T>(this IAsyncDocumentSession session, T entity)
		{
			var properties = session.Advanced.DocumentStore.GetUniquePropertiesForType(typeof(T));

			if (properties != null)
			{
				var typeName = session.Advanced.DocumentStore.Conventions.GetTypeTagName(typeof(T));

				var constraintsIds = (from property in properties
									  let propertyValue = property.GetValue(entity)
									  where propertyValue != null
									  from item in propertyValue is IEnumerable && propertyValue.GetType() != typeof(string) ? ((IEnumerable)propertyValue).Cast<object>().Where(i => i != null) : new[] { propertyValue }
									  select new
									  {
										  Id = "UniqueConstraints/" + typeName.ToLowerInvariant() + "/" + property.Configuration.Name.ToLowerInvariant() + "/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(item.ToString(), property.Configuration.CaseInsensitive),
										  Key = Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(item.ToString(), property.Configuration.CaseInsensitive)
									  }).ToList();

				return session
					.Include<ConstraintDocument>(x => x.RelatedId)
					.Include("Constraints.$Values,RelatedId(ConstraintDocuments/)")
					.LoadAsync(constraintsIds.Select(x => x.Id).ToArray())
							  .ContinueWith(task =>
							  {
								  var constraintDocs = task.Result;
								  var existingDocsIds = new List<string>();
								  for (var i = 0; i < constraintDocs.Length; i++)
								  {
									  var constraintDoc = constraintDocs[i];
									  if (constraintDoc == null)
										  continue;

									  var constraintId = constraintsIds[i];
									  var relatedId = constraintDoc.GetRelatedIdFor(constraintId.Key);

									  if (!string.IsNullOrEmpty(relatedId))
										  existingDocsIds.Add(relatedId);
								  }

								  if (existingDocsIds.Any() == false)
								  {
									  return new CompletedTask<UniqueConstraintCheckResult<T>>(new UniqueConstraintCheckResult<T>(entity, properties, null));
								  }

								  return session.LoadAsync<T>(existingDocsIds.ToArray()).ContinueWith(loadTask =>
								  {
									  var completedTask = new CompletedTask<UniqueConstraintCheckResult<T>>(
										  new UniqueConstraintCheckResult<T>(entity, properties, loadTask.Result)
										  );

									  return (Task<UniqueConstraintCheckResult<T>>)completedTask;
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
			var body = (MemberExpression)keySelector.Body;
			var prop = (PropertyInfo)body.Member;
			object doc;
			propertyDocuments.TryGetValue(prop.Name, out doc);
			return (T)doc;
		}
	}
}
