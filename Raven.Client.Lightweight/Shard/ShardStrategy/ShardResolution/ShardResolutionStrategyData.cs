using System;

namespace Raven.Client.Shard.ShardStrategy.ShardResolution
{
	/// <summary>
	/// Information required to resolve the appropriate shard for an entity / entity and key
	/// </summary>
    public class ShardResolutionStrategyData
    {
		/// <summary>
		/// Builds an instance of <see cref="ShardResolutionStrategyData"/> from the given type
		/// </summary>
        public static ShardResolutionStrategyData BuildFrom(Type type) 
        {
        	return BuildFrom(type, null);
        }

		/// <summary>
		/// Builds an instance of <see cref="ShardResolutionStrategyData"/> from the given type
		/// and key
		/// </summary>
		public static ShardResolutionStrategyData BuildFrom(Type type, string key)
		{
			if (type == null)
				throw new ArgumentNullException("type");

			return new ShardResolutionStrategyData
			{
				EntityType = type,
				Key = key
			};
		}

    	private ShardResolutionStrategyData()
    	{
    		
    	}

		/// <summary>
		/// Gets or sets the key.
		/// </summary>
		/// <value>The key.</value>
		public string Key { get; set; }

		/// <summary>
		/// Gets or sets the type of the entity.
		/// </summary>
		/// <value>The type of the entity.</value>
        public Type EntityType { get; set; }
    }
}
