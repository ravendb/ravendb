// -----------------------------------------------------------------------
//  <copyright file="PersonTransformer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Client.Indexes;
using Raven.Tests.Common.Dto;

namespace Raven.Tests.Web.Models.Transformers
{
    public class PersonTransformer : AbstractTransformerCreationTask<Person>
    {
        public PersonTransformer()
        {
            TransformResults = results => from result in results select result;
        }
    }
}
