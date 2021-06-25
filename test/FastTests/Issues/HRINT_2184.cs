using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using Raven.Client.Json;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class HRINT_2184 : RavenTestBase
    {
        public HRINT_2184(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Validate_ExpressionHelper_ClearList()
        {
            var item = new Item();

            var list = item.GetList();
            Assert.Null(list);

            var clear = ExpressionHelper.SafelyClearList<Item>("_list");

            clear(item); // clearing null

            list = item.GetList();
            Assert.Null(list);

            item.Add();
            item.Add();

            list = item.GetList();
            Assert.NotNull(list);
            Assert.Equal(2, list.Count);

            clear(item);

            list = item.GetList();
            Assert.NotNull(list);
            Assert.Equal(0, list.Count);
        }

        [Fact]
        public void Clear_BlittableJsonReader_Stack_On_Init_To_Avoid_MaxDepth_Exception()
        {
            using (var store = GetDocumentStore())
            {
                var serializer = new NewtonsoftJsonBlittableEntitySerializer(store.Conventions.Serialization);

                using (var session = store.OpenSession())
                {
                    var order = new Order();

                    order.Lines = new List<OrderLine>();

                    for (var i = 0; i < 128; i++)
                        order.Lines.Add(new OrderLine
                        {
                            ProductName = "P" + i
                        });

                    session.Store(order, "orders/1");
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var order = commands.Get("orders/1");
                    var orderJson = order.BlittableJson;

                    var orderEntity = serializer.EntityFromJsonStream(typeof(Order), orderJson);

                    var reader = serializer.GetReaderForCurrentThread();
                    FillStack(reader, 256);

                    orderEntity = serializer.EntityFromJsonStream(typeof(Order), orderJson);
                }

                static void FillStack(BlittableJsonReader reader, int numberOfItems)
                {
                    var stackField = reader.GetType().BaseType.GetField("_stack", BindingFlags.Instance | BindingFlags.NonPublic);
                    var stackFieldItem = stackField.FieldType.GetGenericArguments()[0];

                    var stack = (IList)stackField.GetValue(reader);

                    while (stack.Count < numberOfItems)
                    {
                        var item = Activator.CreateInstance(stackFieldItem);
                        var typeField = item.GetType().GetField("Type", BindingFlags.Instance | BindingFlags.NonPublic);
                        typeField.SetValue(item, 1); // :)

                        var propertyNameField = item.GetType().GetField("PropertyName", BindingFlags.Instance | BindingFlags.NonPublic);
                        propertyNameField.SetValue(item, $"Field_{stack.Count}");

                        stack.Add(item);
                    }
                }
            }
        }

        private class Item
        {
            private List<object> _list;

            public void Add()
            {
                if (_list == null)
                    _list = new List<object>();

                _list.Add(new object());
            }

            public List<object> GetList()
            {
                return _list;
            }
        }
    }
}
