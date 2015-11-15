// -----------------------------------------------------------------------
//  <copyright file="Users_ByName.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common.Dto;

namespace Raven.Tests.Web.Models.Indexes
{
    public class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from u in users select new { Name = u.Name };

            Indexes.Add(x => x.Name, FieldIndexing.Analyzed);

            IndexSuggestions.Add(x => x.Name, new SuggestionOptions());

            Stores.Add(x => x.Name, FieldStorage.Yes);
        }
    }
}
