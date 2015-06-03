﻿// -----------------------------------------------------------------------
//  <copyright file="Animal_Stats_Scripts.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client.Indexes;

namespace Raven.Tests.Bundles.ScriptedIndexResults
{
    public class Animal_Stats_Scripts : AbstractScriptedIndexCreationTask<Animals_Stats>
    {
        public Animal_Stats_Scripts()
        {
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
