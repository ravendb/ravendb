// -----------------------------------------------------------------------
//  <copyright file="ConfigurationModification.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

using Raven.Abstractions.Extensions;
using Raven.Database.Config;
using Raven.Database.Config.Categories;

namespace Raven.Tests.Helpers.Util
{
    public class ConfigurationModification
    {
        private readonly RavenConfiguration configuration;

        public ConfigurationModification(RavenConfiguration configuration)
        {
            this.configuration = configuration;
        }

        public void Modify<TValue>(Expression<Func<RavenConfiguration, TValue>> key, TValue value, string customStringValue = null)
        {
            var expression = key.Body as MemberExpression;

            var members = new Stack<MemberInfo>();

            while (expression != null)
            {
                members.Push(expression.Member);

                expression = expression.Expression as MemberExpression;
            }

            object entity = configuration;

            foreach (var member in members)
            {
                var entityObject = member.GetValue(entity);

                if (entityObject is ConfigurationCategory)
                {
                    entity = entityObject;
                }
                else
                {
                    member.SetValue(entity, value);
                }
            }

            using (configuration.AllowChangeAfterInit())
            {
                configuration.SetSetting(RavenConfiguration.GetKey(key), customStringValue ?? value.ToString());
            }
        }

        public RavenConfiguration Get()
        {
            return configuration;
        }
    }
}