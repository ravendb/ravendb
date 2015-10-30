using System;
using Raven.Client.Converters;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB2643 : RavenTestBase
    {
        [Fact]
        public void SaveDocWithNoStringIdentifierUsingIdentity()
        {
            using (var store = NewDocumentStore())
            {
                store.Conventions.IdentityTypeConvertors.RemoveAll(converter => converter is Int32Converter);
                store.Conventions.IdentityTypeConvertors.Add(new NumericNullableConverter<int>());

                try
                {
                    using (var session = store.OpenSession())
                    {
                        var product = new Product
                        {
                            Name = "Car",
                            Cost = 45
                        };
                        session.Store(product, "products/");
                        session.SaveChanges();

                        Assert.True(product.Id > 0);
                        Assert.True(product.Id == 1);
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception("Error on saving new document with no string identifier.", ex);
                }
            }
        }

        public class Product
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public decimal Cost { get; set; }
        }



        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TValue"></typeparam>
        public class NumericNullableConverter<TValue>
            : ITypeConverter
            where TValue : struct
        {

            private readonly Type type;

            /// <summary>
            /// 
            /// </summary>
            public NumericNullableConverter()
            {
                this.type = typeof(TValue?);
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="sourceType"></param>
            /// <returns></returns>
            public bool CanConvertFrom(Type sourceType)
            {
                //return this.type == sourceType;
                return this.type.IsAssignableFrom(sourceType);
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="tag"></param>
            /// <param name="value"></param>
            /// <param name="allowNull"></param>
            /// <returns></returns>
            public string ConvertFrom(string tag, object value, bool allowNull)
            {
                if (value == null)
                    return null;

                Type valType = value.GetType();

                if (!this.type.IsAssignableFrom(valType) || default(TValue).Equals(value))
                    return tag;

                return tag + value;
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="value"></param>
            /// <returns></returns>
            public object ConvertTo(string value)
            {
                if (string.IsNullOrWhiteSpace(value))
                    return null;

                try
                {
                    return Convert.ChangeType(value, typeof(TValue));
                }
                catch (Exception)
                {
                    return null;
                }
            }
        }

    }

}
