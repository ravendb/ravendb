using System.Collections.Generic;
using Raven.Tests.Core.Utils.Entities;

namespace BenchmarkTests.Utils
{
    public static class EntityFactory
    {
        public static Company CreateCompanySmall(int i)
        {
            return CreateCompanySmall(i, i);
        }

        public static Company CreateCompanySmall(int i, int j)
        {
            return new Company
            {
                Name = $"Hibernating Rhinos {i} {j}",
                Address1 = "Ha'Lamish 14",
                Address2 = "Caesarea Northern Industrial Park",
                Address3 = "Israel",
                Email = "support@hibernatingrhinos.com",
                Type = Company.CompanyType.Private
            };
        }

        public static Company CreateCompanyLarge(int i)
        {
            return CreateCompanyLarge(i, i);
        }

        public static Company CreateCompanyLarge(int i, int j)
        {
            return new Company
            {
                Name = $"Hibernating Rhinos {i} {j}",
                Address1 = "Ha'Lamish 14",
                Address2 = "Caesarea Northern Industrial Park",
                Address3 = "Israel",
                Email = "support@hibernatingrhinos.com",
                Type = Company.CompanyType.Private,
                Desc =
                    $"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Proin sem ipsum, sollicitudin eu ex eget, consectetur ultricies nunc. In vulputate mi libero. Nunc lobortis metus eget ultricies pretium. Aenean vitae ligula ac justo ultrices dapibus volutpat vitae ligula. Praesent dictum ornare lectus, quis pretium nibh volutpat tempor. Sed pharetra quis leo sed dapibus. Maecenas viverra lorem sit amet leo pellentesque viverra. Proin nisi justo, viverra a nisi eget, cursus aliquam erat. Pellentesque pretium sed felis eget eleifend. Vestibulum mauris diam, pretium sit amet imperdiet vel, ultricies ac mi. Quisque iaculis elit mi, sed mollis urna facilisis vel. {i} {j}",
                Contacts = new List<Contact>
                {
                    new Contact
                    {
                        Email = $"support_0_{i}_{j}@hibernatingrhinos.com",
                        FirstName = $"John_0_{i}_{j}",
                        Surname = $"Doe_0_{i}_{j}"
                    },
                    new Contact
                    {
                        Email = $"support_1_{i}_{j}@hibernatingrhinos.com",
                        FirstName = $"John_1_{i}_{j}",
                        Surname = $"Doe_1_{i}_{j}"
                    },
                    new Contact
                    {
                        Email = $"support_2_{i}_{j}@hibernatingrhinos.com",
                        FirstName = $"John_2_{i}_{j}",
                        Surname = $"Doe_2_{i}_{j}"
                    },
                    new Contact
                    {
                        Email = $"support_3_{i}_{j}@hibernatingrhinos.com",
                        FirstName = $"John_3_{i}_{j}",
                        Surname = $"Doe_3_{i}_{j}"
                    }
                }
            };
        }
    }
}
