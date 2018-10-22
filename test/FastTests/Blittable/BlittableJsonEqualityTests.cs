using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Blittable
{
    public class BlittableJsonEqualityTests : NoDisposalNeeded
    {
        [Fact]
        public void Equals_even_though_order_of_properties_is_different()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var json1 = new DynamicJsonValue()
                {
                    ["Age"] = 30,
                    ["Pie"] = 3.147,
                    ["Numbers"] = new DynamicJsonArray()
                    {
                        1, 2, null, 3
                    },
                    ["Address"] = new DynamicJsonValue()
                    {
                        ["City"] = "Atlanta",
                        ["ZipCode"] = 1234
                    },
                    ["Friends"] = new DynamicJsonArray()
                    {
                        new DynamicJsonValue()
                        {
                            ["Name"] = "James",
                            ["ZipCode"] = 999
                        }
                    },
                    ["Tags"] = null
                };

                var json2 = new DynamicJsonValue()
                {
                    ["Pie"] = 3.147,
                    ["Age"] = 30,
                    ["Address"] = new DynamicJsonValue()
                    {
                        ["ZipCode"] = 1234,
                        ["City"] = "Atlanta"
                    },
                    ["Numbers"] = new DynamicJsonArray()
                    {
                        1, 2, null, 3
                    },
                    ["Tags"] = null,
                    ["Friends"] = new DynamicJsonArray()
                    {
                        new DynamicJsonValue()
                        {
                            ["Name"] = "James",
                            ["ZipCode"] = 999
                        }
                    },
                };

                using (var blittable1 = ctx.ReadObject(json1, "foo"))
                using (var blittable2 = ctx.ReadObject(json2, "foo"))
                {
                    Assert.Equal(blittable1, blittable2);
                }
            }
        }

        [Fact]
        public void GetHashCode_must_not_use_size_if_it_isnt_root()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var json1 = new DynamicJsonValue()
                {
                    ["Address"] = new DynamicJsonValue()
                    {
                        ["City"] = "Atlanta",
                        ["ZipCode"] = 1234
                    },
                    ["Friends"] = "yes"
                };

                var json2 = new DynamicJsonValue()
                {

                    ["Address"] = new DynamicJsonValue()
                    {
                        ["ZipCode"] = 1234,
                        ["City"] = "Atlanta"
                    },
                    ["Friends"] = "no"
                };

                using (var blittable1 = ctx.ReadObject(json1, "foo"))
                using (var blittable2 = ctx.ReadObject(json2, "foo"))
                {
                    blittable1.TryGet("Address", out BlittableJsonReaderObject ob1);
                    blittable2.TryGet("Address", out BlittableJsonReaderObject ob2);

                    HashSet<BlittableJsonReaderObject> items = new HashSet<BlittableJsonReaderObject>();

                    Assert.True(items.Add(ob1));
                    Assert.False(items.Add(ob2));

                    Assert.Equal(ob1.GetHashCode(), ob2.GetHashCode());
                }
            }
        }

        [Fact]
        public void Equals_when_creating_blittable_in_different_ways()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(ctx))
                using (var embeddedBuilder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(ctx))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    embeddedBuilder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);

                    embeddedBuilder.StartWriteObjectDocument();
                    embeddedBuilder.StartWriteObject();

                    embeddedBuilder.WritePropertyName("Name");
                    embeddedBuilder.WriteValue("Hibernating Rhinos");
                    embeddedBuilder.WritePropertyName("Type");
                    embeddedBuilder.WriteValue("LTD");

                    embeddedBuilder.WriteObjectEnd();
                    embeddedBuilder.FinalizeDocument();
                    var embeddedCompany = embeddedBuilder.CreateReader();

                    builder.StartWriteObjectDocument();
                    builder.StartWriteObject();

                    builder.WritePropertyName("Company");
                    builder.WriteEmbeddedBlittableDocument(embeddedCompany);
                    builder.WritePropertyName("Street");
                    builder.WriteValue("Hanasi 21");
                    builder.WritePropertyName("City");
                    builder.WriteValue("Hadera");

                    builder.WriteObjectEnd();
                    builder.FinalizeDocument();
                    var blittable1 = builder.CreateReader();

                    var json2 = new DynamicJsonValue()
                    {
                        ["Company"] = new DynamicJsonValue()
                        {
                            ["Name"] = "Hibernating Rhinos",
                            ["Type"] = "LTD",
                        },
                        ["Street"] = "Hanasi 21",
                        ["City"] = "Hadera",
                    };

                    using (var blittable2 = ctx.ReadObject(json2, "foo"))
                    {
                        Assert.Equal(blittable1, blittable2);
                        Assert.Equal(blittable1.GetHashCode(), blittable2.GetHashCode());

                        blittable1.TryGet("Company", out BlittableJsonReaderObject ob1);
                        blittable2.TryGet("Company", out BlittableJsonReaderObject ob2);

                        Assert.Equal(ob1, ob2);
                    }
                }
            }
        }

        [Fact]
        public void Equals_blittables_created_manually()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                BlittableJsonReaderObject CreateBlittable()
                {
                    using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(ctx))
                    using (var officeBuilder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(ctx))
                    using (var companyBuilder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(ctx))
                    {
                        builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                        officeBuilder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                        companyBuilder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);

                        companyBuilder.StartWriteObjectDocument();
                        companyBuilder.StartWriteObject();

                        companyBuilder.WritePropertyName("Name");
                        companyBuilder.WriteValue("Hibernating Rhinos");
                        companyBuilder.WritePropertyName("Type");
                        companyBuilder.WriteValue("LTD");

                        companyBuilder.WriteObjectEnd();
                        companyBuilder.FinalizeDocument();
                        var embeddedCompany = companyBuilder.CreateReader();

                        officeBuilder.StartWriteObjectDocument();
                        officeBuilder.StartWriteObject();

                        officeBuilder.WritePropertyName("Company");
                        officeBuilder.WriteEmbeddedBlittableDocument(embeddedCompany);
                        officeBuilder.WritePropertyName("Street");
                        officeBuilder.WriteValue("Hanasi 21");
                        officeBuilder.WritePropertyName("City");
                        officeBuilder.WriteValue("Hadera");

                        officeBuilder.WriteObjectEnd();
                        officeBuilder.FinalizeDocument();

                        var embeddedOffice = officeBuilder.CreateReader();

                        builder.StartWriteObjectDocument();
                        builder.StartWriteObject();

                        builder.WritePropertyName("Office");
                        builder.WriteEmbeddedBlittableDocument(embeddedOffice);

                        builder.WriteObjectEnd();
                        builder.FinalizeDocument();

                        return builder.CreateReader();
                    }
                }

                using (var blittable1 = CreateBlittable())
                using (var blittable2 = CreateBlittable())
                {
                    Assert.Equal(blittable1, blittable2);
                    Assert.Equal(blittable1.GetHashCode(), blittable2.GetHashCode());

                    blittable1.TryGet("Office", out BlittableJsonReaderObject ob1);
                    blittable2.TryGet("Office", out BlittableJsonReaderObject ob2);

                    Assert.Equal(ob1, ob2);
                }
            }
        }
    }
}
