//-----------------------------------------------------------------------
// <copyright file="ShardResolutionStrategyData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Client.Shard.ShardStrategy.ShardResolution
{
    /// <summary>
    /// Information required to resolve the appropriate shard for an entity / entity and key
    /// </summary>
    public class ShardResolutionStrategyData
    {
        private string key;

        private ValueType valueTypeKey;

        private ShardResolutionStrategyData()
        {
        }

        /// <summary>
        /// Gets or sets the key.
        /// </summary>
        /// <value>The key.</value>
        public string Key
        {
            get
            {
                if (HasValueTypeKey)
                    throw new InvalidOperationException("You did not check HasStringTypeKey. These data might contain a ValueType key. Check and use that property instead.");

                return key;
            }
            private set { key = value; }
        }

        /// <summary>
        /// Gets the key.
        /// </summary>
        public ValueType ValueTypeKey
        {
            get
            {
                if (HasStringTypeKey)
                    throw new InvalidOperationException("You did not check HasValueTypeKey. These data might contain a string key. Check and use that property instead.");

                return valueTypeKey;
            }
            private set { valueTypeKey = value; }
        }

        /// <summary>
        /// Gets or sets the type of the entity.
        /// </summary>
        /// <value>The type of the entity.</value>
        public Type EntityType { get; private set; }

        /// <summary>
        /// Indicates that <see cref="Key"/> has to be used for shard selection logic.
        /// </summary>
        public bool HasStringTypeKey { get; private set; }

        /// <summary>
        /// Indicates that <see cref="ValueTypeKey"/> has to be used for shard selection logic.
        /// </summary>
        public bool HasValueTypeKey { get; private set; }

        /// <summary>
        /// Builds an instance of <see cref="ShardResolutionStrategyData"/> from the given type
        /// </summary>
        public static ShardResolutionStrategyData BuildFrom(Type type)
        {
            return BuildFrom(type, (string) null);
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
                           HasStringTypeKey = true,
                           HasValueTypeKey = false,
                           EntityType = type,
                           Key = key
                       };
        }

        /// <summary>
        /// Builds an instance of <see cref="ShardResolutionStrategyData"/> from the given type
        /// and key
        /// </summary>
        public static ShardResolutionStrategyData BuildFrom(Type type, ValueType key)
        {
            if (type == null)
                throw new ArgumentNullException("type");

            return new ShardResolutionStrategyData
                       {
                           HasStringTypeKey = false,
                           HasValueTypeKey = true,
                           EntityType = type,
                           ValueTypeKey = key
                       };
        }
    }
}