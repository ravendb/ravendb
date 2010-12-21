//-----------------------------------------------------------------------
// <copyright file="FailDelete.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;
using Raven.Client;

namespace Raven.Tests.Bugs
{
    public class FailDelete : IDocumentDeleteListener
    {
        public void BeforeDelete(string key, object entityInstance, JObject metadata)
        {
            throw new NotImplementedException();
        }
    }
}