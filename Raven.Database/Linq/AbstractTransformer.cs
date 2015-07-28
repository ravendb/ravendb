using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Database.Indexing;
using Raven.Json.Linq;
using ValueType = System.ValueType;

namespace Raven.Database.Linq
{
	public abstract class AbstractTransformer
	{
		private TransformerDefinition transformerDefinition;
		private byte[] cachedBytes;
		public IndexingFunc TransformResultsDefinition { get; set; }
		public string SourceCode { get; set; }

		public string Name { get { return transformerDefinition.Name; } }
		public string ViewText { get; set; }

		[CLSCompliant(false)]
		// ReSharper disable once InconsistentNaming
		protected DynamicNullObject __dynamic_null = new DynamicNullObject();


		public IEnumerable<dynamic> TransformWith(IEnumerable<string> transformers, dynamic maybeItems)
		{
			return Enumerable.Aggregate(transformers, maybeItems,
				(Func<dynamic, string, dynamic>)((items, transformer) => TransformWith(transformer, items)));
		}

		public IEnumerable<dynamic> TransformWith(string transformer, dynamic maybeItems)
		{
			if (CurrentTransformationScope.Current == null)
				throw new InvalidOperationException("TransformWith was accessed without CurrentTransformationScope.Current being set");

			if (CurrentTransformationScope.Current.Nested.Add(transformer) == false)
				throw new InvalidOperationException("Cannot call transformer " + transformer + " because it was already called, recursive transformers are not allowed. Current transformers are: " + string.Join(", ", CurrentTransformationScope.Current.Nested));
			try
			{
				var storedTransformer = CurrentTransformationScope.Current.Database.IndexDefinitionStorage.GetTransformer(transformer);
				if (storedTransformer == null)
					throw new InvalidOperationException("No transformer with the name: " + transformer);

				var enumerable = maybeItems as IEnumerable;
				var objects = enumerable != null && AnonymousObjectToLuceneDocumentConverter.ShouldTreatAsEnumerable(enumerable) ? 
					enumerable.Cast<dynamic>() : new[] {maybeItems};

				foreach (var result in AllowAccessToResultsEvenIfTheyAreStupidInternalAnonymousTypes(storedTransformer.TransformResultsDefinition(objects)))
				{
					yield return result;
				}
			}
			finally
			{
				CurrentTransformationScope.Current.Nested.Remove(transformer);
			}
		}

		// need to work around this: http://www.heartysoft.com/ashic/blog/2010/5/anonymous-types-c-sharp-4-dynamic
		private IEnumerable<object> AllowAccessToResultsEvenIfTheyAreStupidInternalAnonymousTypes(IEnumerable<object> items)
		{
			foreach (var item in items)
			{
				if (item == null)
					yield return new DynamicNullObject();
				if (item is ValueType ||
					item is string ||
					item is RavenJToken ||
					item is DynamicJsonObject ||
					item is DynamicNullObject ||
					item is IDictionary)
					yield return item;
				// assume that this is anonymous type, hence all internals, hence can't be access by the calling transformer
				var json = RavenJObject.FromObject(item);
				yield return new DynamicJsonObject(json);
			}
		}

		// Required for RavenDB-1519
		protected dynamic LoadDocument<TIgnored>(object key)
		{
			return LoadDocument(key);
		}

		protected dynamic LoadDocument(object key)
		{
			if (CurrentTransformationScope.Current == null)
				throw new InvalidOperationException("LoadDocument was called without CurrentTransformationScope.Current being set: " + key);

			return CurrentTransformationScope.Current.Retriever.Load(key);
		}

		[Obsolete("Use Parameter instead.")]
		protected RavenJToken Query(string key)
		{
			return Parameter(key);
		}

		[Obsolete("Use ParameterOrDefault instead.")]
		protected RavenJToken QueryOrDefault(string key, object val)
		{
			return ParameterOrDefault(key, val);
		}

		protected RavenJToken Parameter(string key)
		{
			if (CurrentTransformationScope.Current == null)
				throw new InvalidOperationException("Query was accessed without CurrentTransformationScope.Current being set");

			RavenJToken value;
			if (CurrentTransformationScope.Current.Retriever.TransformerParameters.TryGetValue(key, out value) == false)
				throw new InvalidOperationException("Query parameter " + key + " was accessed, but it wasn't provided for this query.");
			return value;

		}

		protected RavenJToken ParameterOrDefault(string key, object val)
		{
			if (CurrentTransformationScope.Current == null)
				throw new InvalidOperationException("Query was accessed without CurrentTransformationScope.Current being set");

			RavenJToken value;
			if (CurrentTransformationScope.Current.Retriever.TransformerParameters.TryGetValue(key, out value) == false)
				return RavenJToken.FromObject(val);
			return value;

		}

		public object Include(object key)
		{
			if (CurrentTransformationScope.Current == null)
				throw new InvalidOperationException("Include was called without CurrentTransformationScope.Current being set: " + key);

			return CurrentTransformationScope.Current.Retriever.Include(key);

		}

		protected IEnumerable<dynamic> Recurse(object item, Func<dynamic, dynamic> func)
		{
			return new RecursiveFunction(item, func).Execute();
		}

		public void Init(TransformerDefinition def)
		{
			transformerDefinition = def;
		}

		public byte[] GetHashCodeBytes()
		{
			if (cachedBytes != null)
				return cachedBytes;
			return cachedBytes = BitConverter.GetBytes(GetHashCode());
		}

		protected bool Equals(AbstractTransformer other)
		{
			return Equals(transformerDefinition, other.transformerDefinition);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((AbstractTransformer)obj);
		}

		public override int GetHashCode()
		{
			return (transformerDefinition != null ? transformerDefinition.GetHashCode() : 0);
		}
	}
}