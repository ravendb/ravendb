using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    /// <summary>
    /// If Transformer can not find entity then should return null rather than NullReference
    /// </summary>
    public class RavenDB1259 : RavenTestBase
    {
        [Fact]
        public void Should_throw_invalid_operation_exception_when_production_area_does_not_exist_and_using_transformer()
        {
            using (var store = NewDocumentStore())
            {
                new ProductionAreaTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    // Arrange
                    var productionArea = new ProductionArea { Name = "Production Area 1" };
                    session.Store(productionArea);
                    var productionAreaId = productionArea.Id;
                    session.SaveChanges();

                    var service = new ProducerService(session);

                    // Act
                    const string badProductionAreaId = "ProductionAreas-999";                    

                    // Assert
                    Assert.Throws<InvalidOperationException>(() => service.GetProductionAreaWithTransformer(badProductionAreaId));
                }
            }
        }


        [Fact]
        public void Should_throw_invalid_operation_exception_when_production_area_does_not_exist_and_not_using_transformer()
        {
            using (var store = NewDocumentStore())
            {
                new ProductionAreaTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    // Arrange
                    var productionArea = new ProductionArea { Name = "Production Area 1" };
                    session.Store(productionArea);
                    var productionAreaId = productionArea.Id;
                    session.SaveChanges();

                    var service = new ProducerService(session);

                    // Act
                    const string badProductionAreaId = "ProductionAreas-9999";

                    // Assert
                    Assert.Throws<InvalidOperationException>(() => service.GetProductionAreaWithoutTransformer(badProductionAreaId));
                }
            }
        }

        [Fact]
        public void Should_return_production_area()
        {
            using (var store = NewDocumentStore())
            {
                new ProductionAreaTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    // Arrange
                    var productionArea = new ProductionArea { Name = "Production Area 1" };
                    session.Store(productionArea);
                    var productionAreaId = productionArea.Id;
                    session.SaveChanges();

                    var service = new ProducerService(session);

                    // Act
                    var dto = service.GetProductionAreaWithTransformer(productionAreaId);

                    // Assert
                    Assert.NotNull(dto);                   
                }
            }
        }

        public class ProducerService
        {
            private readonly IDocumentSession _session;

            public ProducerService(IDocumentSession session)
            {
                _session = session;
            }

            public ProductionAreaDto GetProductionAreaWithTransformer(string productionAreaId)
            {
                var productionArea = _session.Load<ProductionAreaTransformer, ProductionAreaDto>(productionAreaId); //before issue fix - this line threw NullReferenceException

                if (productionArea == null)
                {
                    throw new InvalidOperationException("Can not find ProductionArea with Id: " + productionAreaId);
                }

                return productionArea;
            }

            public ProductionAreaDto GetProductionAreaWithoutTransformer(string productionAreaId)
            {
                var productionArea = _session.Load<ProductionArea>(productionAreaId);
                if (productionArea == null)
                {
                    throw new InvalidOperationException("Can not find ProductionArea with Id: " + productionAreaId);
                }

                var dto = new ProductionAreaDto
                {
                    Name = productionArea.Name
                };

                return dto;
            }
        }

        public class ProductionArea
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class ProductionAreaDto
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class ProductionAreaTransformer : AbstractTransformerCreationTask<ProductionArea>
        {
            public ProductionAreaTransformer()
            {
                TransformResults = productionAreas => from c in productionAreas
                                                      select new
                                                      {
                                                          c.Id,
                                                          c.Name
                                                      };
            }
        }
    }
}
