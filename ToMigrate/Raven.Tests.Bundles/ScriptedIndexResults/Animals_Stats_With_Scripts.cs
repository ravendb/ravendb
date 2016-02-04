// -----------------------------------------------------------------------
//  <copyright file="Animals_Stats_With_Scripts.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Client.Indexes;

namespace Raven.Tests.Bundles.ScriptedIndexResults
{
    public class Animals_Stats_With_Scripts : AbstractScriptedIndexCreationTask<Animal, Animals_Stats.Result>
    {
        public class Result
        {
            public string Type { get; set; }
            public int Count { get; set; }
        }
        public Animals_Stats_With_Scripts()
        {
            Map = animals =>
                  from animal in animals
                  select new
                  {
                      animal.Type,
                      Count = 1
                  };

            Reduce = animals =>
                     from result in animals
                     group result by result.Type
                         into g
                         select new
                         {
                             Type = g.Key,
                             Count = g.Sum(x => x.Count)
                         };

            IndexScript = @"
var docId = 'AnimalTypes/'+ key;
var type = LoadDocument(docId) || {};
type.Count = this.Count;
PutDocument(docId, type);";

            DeleteScript = @"
var docId = 'AnimalTypes/'+ key;
var type = LoadDocument(docId);
if(type == null)
    return;
type.Count = 0;
PutDocument(docId, type);
";
        }
    }
}
