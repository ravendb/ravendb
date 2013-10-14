using System;
using System.Collections.Generic;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Raven.Json.Linq;

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

		protected dynamic LoadDocument(object key)
		{
			if (CurrentTransformationScope.Current == null)
				throw new InvalidOperationException("LoadDocument was called without CurrentTransformationScope.Current being set: " + key);

			return CurrentTransformationScope.Current.Load(key);
		}

	    protected RavenJToken Query(string key)
	    {
            if (CurrentTransformationScope.Current == null)
                throw new InvalidOperationException("Query was accessed without CurrentTransformationScope.Current being set");

	        RavenJToken value;
	        if(CurrentTransformationScope.Current.QueryInputs.TryGetValue(key, out value) == false)
                throw new InvalidOperationException("Query parameter "+key+ " was accessed, but it wasn't provided for this query.");
	        return value;

	    }

	    public object Include(object key)
		{
			if (CurrentTransformationScope.Current == null)
				throw new InvalidOperationException("Include was called without CurrentTransformationScope.Current being set: " + key);

			return CurrentTransformationScope.Current.Include(key);
	
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
			return Equals((AbstractTransformer) obj);
		}

		public override int GetHashCode()
		{
			return (transformerDefinition != null ? transformerDefinition.GetHashCode() : 0);
		}
	}
}