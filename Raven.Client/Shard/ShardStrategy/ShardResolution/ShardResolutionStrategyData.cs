using System;

namespace Raven.Client.Shard.ShardStrategy.ShardResolution
{
    public class ShardResolutionStrategyData
    {
        public static ShardResolutionStrategyData BuildFrom(Type type) 
        {
        	return BuildFrom(type, null);
        }


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

		public string Key { get; set; }

        public Type EntityType { get; set; }
    }
}
