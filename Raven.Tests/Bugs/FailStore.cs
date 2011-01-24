//-----------------------------------------------------------------------
// <copyright file="FailStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;
using Raven.Client;

namespace Raven.Tests.Bugs
{
    public class FailStore : IDocumentStoreListener
    {
        public void BeforeStore(string key, object entityInstance, JObject metadata)
        {
            throw new NotImplementedException();
        }

        public void AfterStore(string key, object entityInstance, JObject metadata)
        {
            throw new NotImplementedException();
        }
    }
}