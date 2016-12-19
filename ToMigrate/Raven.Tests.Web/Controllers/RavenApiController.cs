// -----------------------------------------------------------------------
//  <copyright file="RavenApiController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Http;

using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Web.Models;

namespace Raven.Tests.Web.Controllers
{
    public abstract class RavenApiController : ApiController
    {
        private static readonly Lazy<IDocumentStore> DocumentStoreLazy = new Lazy<IDocumentStore>(() =>
        {
            var store = new EmbeddableDocumentStore
                        {
                            RunInMemory = true,
                            Configuration =
                            {
                                Storage =
                                {
                                    Voron =
                                    {
                                        AllowOn32Bits = true
                                    }
                                }
                            }
                        }.Initialize();

            new RavenDocumentsByEntityName().Execute(store);

            IndexCreation.CreateIndexes(typeof(RavenApiController).Assembly, store);

            return store;
        });

        protected IDocumentStore DocumentStore
        {
            get
            {
                return DocumentStoreLazy.Value;
            }
        }

        [HttpGet]
        public IList<ApiControllerMethod> Methods()
        {
            var type = GetType();

            var apiDescriptions = GlobalConfiguration
                .Configuration
                .Services
                .GetApiExplorer()
                .ApiDescriptions
                .Where(x => x.ActionDescriptor.ControllerDescriptor.ControllerType == type)
                .ToList();

            return apiDescriptions
                .Where(x => x.ActionDescriptor.ControllerDescriptor.ControllerName != null)
                .Select(x => new ApiControllerMethod
                {
                    Method = x.HttpMethod.ToString(),
                    Route = x.Route.RouteTemplate
                })
                .Where(x => x.Route != "api/{controller}/{id}")
                .ToList();
        }
    }
}
