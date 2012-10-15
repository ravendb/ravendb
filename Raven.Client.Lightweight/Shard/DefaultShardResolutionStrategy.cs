//-----------------------------------------------------------------------
// <copyright file="IShardResolutionStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Shard
{
	public class DefaultShardResolutionStrategy : IShardResolutionStrategy
	{
		private readonly ShardStrategy shardStrategy;

		protected delegate string ShardFieldForQueryingFunc(Type entityType);

		protected readonly List<string> ShardIds;

		private long currentShardCounter;
		private readonly Dictionary<Type, Regex> regexToCaptureShardIdFromQueriesByType = new Dictionary<Type, Regex>();

		private readonly Dictionary<Type, Func<object, string>> shardResultToStringByType = new Dictionary<Type, Func<object, string>>();
		private readonly Dictionary<Type, Func<string, string>> queryResultToStringByType = new Dictionary<Type, Func<string, string>>();

		private SessionMetadata lastSessionMetadata;

		public DefaultShardResolutionStrategy(IEnumerable<string> shardIds, ShardStrategy shardStrategy)
		{
			this.shardStrategy = shardStrategy;
			ShardIds = new List<string>(shardIds);
			if (ShardIds.Count == 0)
				throw new ArgumentException("shardIds must have at least one value", "shardIds");
		}

		public void ShardingOn<TEntity>(Expression<Func<TEntity, string>> shardingProperty, Func<string, string> translator = null)
		{
			ShardingOn(shardingProperty, translator, translator);
		}

		public void ShardingOn<TEntity, TResult>(Expression<Func<TEntity, TResult>> shardingProperty,
			Func<TResult, string> valueTranslator = null,
			Func<string, string> queryTranslator = null
			)
		{
			valueTranslator = valueTranslator ?? (result =>
			{
				if (ReferenceEquals(result, null))
					throw new InvalidOperationException("Got null for the shard id in the value translator for " +
														typeof(TEntity) + " using " + shardingProperty +
														", no idea how to get the shard id from null.");

				// by default we assume that if you have a separator in the value we got back
				// the shard id is the very first value up until the first separator
				var str = result.ToString();
				var start = str.IndexOf(shardStrategy.Conventions.IdentityPartsSeparator, StringComparison.InvariantCultureIgnoreCase);
				if (start == -1)
					return str;
				return str.Substring(0, start);
			});

			queryTranslator = queryTranslator ?? (result => valueTranslator((TResult)Convert.ChangeType(result, typeof(TResult))));

			var shardFieldForQuerying = shardingProperty.ToPropertyPath();

			if (shardStrategy.Conventions.FindIdentityProperty(shardingProperty.ToProperty()))
			{
				shardFieldForQuerying = Constants.DocumentIdFieldName;
			}

			var pattern = string.Format(@"
{0}: \s* (?<Open>"")(?<shardId>[^""]+)(?<Close-Open>"") |
{0}: \s* (?<shardId>[^""][^\s]*)", Regex.Escape(shardFieldForQuerying));

			regexToCaptureShardIdFromQueriesByType[typeof(TEntity)] = new Regex(pattern, RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

			var compiled = shardingProperty.Compile();

			shardResultToStringByType[typeof(TEntity)] = o => valueTranslator(compiled((TEntity)o));
			queryResultToStringByType[typeof(TEntity)] = o => queryTranslator(o);
		}


		/// <summary>
		///  Generate a shard id for the specified entity
		///  </summary>
		public virtual string GenerateShardIdFor(object entity, SessionMetadata sessionMetadata)
		{
			if (shardResultToStringByType.Count == 0)
			{
				bool useDefault = !(this.lastSessionMetadata != null && this.lastSessionMetadata.SaveCounter == sessionMetadata.SaveCounter);

				lastSessionMetadata = sessionMetadata;

				if (useDefault)
				{
					// default, round robin scenario
					var increment = Interlocked.Increment(ref currentShardCounter);
					return ShardIds[(int)increment % ShardIds.Count];
				}
				else
				{
					// one shard per save changes
					var increment = Interlocked.Read(ref currentShardCounter);
					return ShardIds[(int)increment % ShardIds.Count];
				}
			}

			Func<object, string> func;
			if (shardResultToStringByType.TryGetValue(entity.GetType(), out func) == false)
				throw new InvalidOperationException(
					"Entity " + entity.GetType().FullName + " was not setup in " + GetType().FullName + " even though other entities have been setup using ShardingOn<T>()." + Environment.NewLine +
					"Did you forget to call ShardingOn<" + entity.GetType().FullName + ">() and provide the sharding function required?");

			return func(entity);
		}

		/// <summary>
		///  The shard id for the server that contains the metadata (such as the HiLo documents)
		///  for the given entity
		///  </summary>
		public virtual string MetadataShardIdFor(object entity)
		{
			return ShardIds.First();
		}

		/// <summary>
		///  Selects the shard ids appropriate for the specified data.
		///  </summary><returns>Return a list of shards ids that will be search. Returning null means search all shards.</returns>
		public virtual IList<string> PotentialShardsFor(ShardRequestData requestData)
		{
			if (requestData.Query != null)
			{
				Regex regex;
				if (regexToCaptureShardIdFromQueriesByType.TryGetValue(requestData.EntityType, out regex) == false)
					return null; // we have no special knowledge, let us just query everything

				var collection = regex.Matches(requestData.Query.Query);
				if (collection.Count == 0)
					return null; // we don't have the sharding field, we have to query over everything

				var translateQueryValueToShardId = queryResultToStringByType[requestData.EntityType];

				var potentialShardsFor = collection.Cast<Match>().Select(match => translateQueryValueToShardId(match.Groups["shardId"].Value)).ToList();

				if (potentialShardsFor.Any(queryShardId => ShardIds.Contains(queryShardId, StringComparer.InvariantCultureIgnoreCase)) == false)
					return null; // we couldn't find the shard ids here, maybe there is something wrong in the query, sending to all shards

				return potentialShardsFor;
			}

			if (requestData.Keys.Count == 0) // we are only optimized for keys
				return null;


			// we are looking for search by key, let us see if we can narrow it down by using the 
			// embedded shard id.
			var list = new List<string>();
			foreach (var key in requestData.Keys)
			{
				var start = key.IndexOf(shardStrategy.Conventions.IdentityPartsSeparator, StringComparison.InvariantCultureIgnoreCase);
				if (start == -1)
					return null; // if we couldn't figure it out, select from all

				var maybeShardId = key.Substring(0, start);

				if (ShardIds.Any(x => string.Equals(maybeShardId, x, StringComparison.InvariantCultureIgnoreCase)))
					list.Add(maybeShardId);
				else
					return null; // we couldn't find it there, select from all

			}
			return list.ToArray();
		}
	}
}
