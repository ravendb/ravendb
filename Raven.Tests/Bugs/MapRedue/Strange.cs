using Raven.Abstractions;
using Raven.Database.Linq;
using System.Linq;
using System.Collections.Generic;
using System.Collections;
using System;
using Raven.Database.Linq.PrivateExtensions;
using Lucene.Net.Documents;
using Raven.Database.Indexing;
public class Index_IndexWithLetInReduceFunction : AbstractViewGenerator
{
	public Index_IndexWithLetInReduceFunction()
	{
		this.ViewText = @"docs.Users
	.Select(user => new {Id = user.__document_id, Name = user.Name})
results
	.GroupBy(result => result.Id)
	.Select(g => new {g = g, dummy = g.FirstOrDefault(x => x.Name != null)})
	.Select(__h__TransparentIdentifier2 => new {Id = __h__TransparentIdentifier2.g.Key, Name = __h__TransparentIdentifier2.dummy.Name})

";
		this.ForEntityNames.Add("Users");
		this.AddMapDefinition(docs => docs.Select((Func<dynamic, dynamic>)(user => new { Id = user.Id, Name = user.Name, __document_id = user.Id })));
		this.ReduceDefinition = results => results.GroupBy((Func<dynamic, dynamic>)(result => result.Id))
			.Select((Func<IGrouping<dynamic, dynamic>, dynamic>)(g => new { g = g, dummy = g.FirstOrDefault((Func<dynamic, bool>)(x => x.Name != null)) }))
			.Select((Func<dynamic, dynamic>)(__h__TransparentIdentifier2 => new { Id = ((IGrouping<dynamic, dynamic>)__h__TransparentIdentifier2.g).Key, Name = __h__TransparentIdentifier2.dummy.Name }));
		this.GroupByExtraction = result => result.Id;
		this.AddField("Id");
		this.AddField("Name");
	}
}
