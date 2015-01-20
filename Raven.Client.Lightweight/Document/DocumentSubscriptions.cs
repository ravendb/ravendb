// -----------------------------------------------------------------------
//  <copyright file="SubscriptionChannel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
	public class DocumentSubscriptions : IReliableSubscriptions
	{
		private readonly AsyncDocumentSubscriptions innerAsync;

		public DocumentSubscriptions(IDocumentStore documentStore)
		{
			innerAsync = new AsyncDocumentSubscriptions(documentStore);
		}

		public long Create(SubscriptionCriteria criteria, string database = null)
		{
			return innerAsync.CreateAsync(criteria, database).ResultUnwrap();
		}

		public long Create<T>(SubscriptionCriteria<T> criteria, string database = null)
		{
			return innerAsync.CreateAsync(criteria, database).ResultUnwrap();
		}

		public Subscription<RavenJObject> Open(long id, SubscriptionConnectionOptions options, string database = null)
		{
			return innerAsync.OpenAsync(id, options, database).ResultUnwrap();
		}

		public Subscription<T> Open<T>(long id, SubscriptionConnectionOptions options, string database = null) where T : class 
		{
			return innerAsync.OpenAsync<T>(id, options, database).ResultUnwrap();
		}

		public List<SubscriptionConfig> GetSubscriptions(int start, int take, string database = null)
		{
			return innerAsync.GetSubscriptionsAsync(start, take, database).ResultUnwrap();
		}

		public void Delete(long id, string database = null)
		{
			innerAsync.DeleteAsync(id, database).WaitUnwrap();
		}

		public void Release(long id, string database = null)
		{
			innerAsync.ReleaseAsync(id, database).WaitUnwrap();
		}

		public void Dispose()
		{
			innerAsync.Dispose();
		}
	}
}