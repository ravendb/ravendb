// -----------------------------------------------------------------------
//  <copyright file="SmugglerStreamActionsBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

using Raven.Imports.Newtonsoft.Json;

namespace Raven.Smuggler.Common
{
    public abstract class SmugglerStreamActionsBase : IDisposable
    {
        protected JsonTextWriter Writer { get; private set; }

        protected SmugglerStreamActionsBase(JsonTextWriter writer, string sectionName)
        {
            Writer = writer;
            Writer.WritePropertyName(sectionName);
            Writer.WriteStartArray();
        }

        public void Dispose()
        {
            Writer.WriteEndArray();
        }
    }
}
