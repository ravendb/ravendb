using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15753 : RavenTestBase
    {
        public RavenDB_15753(ITestOutputHelper output)
            : base(output)
        {
        }

        [Fact]
        public void AdditionalAssemblies_Runtime()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "XmlIndex",
                    Maps =
                    {
                        "from c in docs.Companies select new { Name = typeof(System.Xml.XmlNode).Name }"
                    },
                    AdditionalAssemblies =
                    {
                        AdditionalAssembly.FromRuntime("System.Xml"),
                        AdditionalAssembly.FromRuntime("System.Xml.ReaderWriter"),
                        AdditionalAssembly.FromRuntime("System.Private.Xml")
                    }
                }));
            }
        }

        [Fact]
        public void AdditionalAssemblies_Runtime_InvalidName()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "XmlIndex",
                    Maps =
                    {
                        "from c in docs.Companies select new { Name = typeof(System.Xml.XmlNode).Name }"
                    },
                    AdditionalAssemblies =
                    {
                        AdditionalAssembly.FromRuntime("Some.Assembly.That.Does.Not.Exist")
                    }
                })));

                Assert.Contains("Cannot load assembly 'Some.Assembly.That.Does.Not.Exist'", e.Message);
                Assert.Contains("Could not load file or assembly 'Some.Assembly.That.Does.Not.Exist", e.Message);
            }
        }

        [Fact]
        public void AdditionalAssemblies_NuGet()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "XmlIndex",
                    Maps =
                    {
                        "from c in docs.Companies select new { Name = typeof(System.Xml.XmlNode).Name }"
                    },
                    AdditionalAssemblies =
                    {
                        AdditionalAssembly.FromRuntime("System.Private.Xml"),
                        AdditionalAssembly.FromNuGet("System.Xml.ReaderWriter", "4.3.1")
                    }
                }));
            }
        }

        [Fact]
        public void AdditionalAssemblies_NuGet_InvalidName()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "XmlIndex",
                    Maps =
                    {
                        "from c in docs.Companies select new { Name = typeof(System.Xml.XmlNode).Name }"
                    },
                    AdditionalAssemblies =
                    {
                        AdditionalAssembly.FromNuGet("Some.Assembly.That.Does.Not.Exist", "4.3.1")
                    }
                })));

                Assert.Contains("Cannot load NuGet package 'Some.Assembly.That.Does.Not.Exist'", e.Message);
                Assert.Contains("NuGet package 'Some.Assembly.That.Does.Not.Exist' version '4.3.1' from 'https://api.nuget.org/v3/index.json' does not exist", e.Message);
            }
        }

        [Fact]
        public void AdditionalAssemblies_NuGet_InvalidSource()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "XmlIndex",
                    Maps =
                    {
                        "from c in docs.Companies select new { Name = typeof(System.Xml.XmlNode).Name }"
                    },
                    AdditionalAssemblies =
                    {
                        AdditionalAssembly.FromNuGet("System.Xml.ReaderWriter", "4.3.1", "http://some.url.that.does.not.exist.com")
                    }
                })));

                Assert.Contains("Cannot load NuGet package 'System.Xml.ReaderWriter' version '4.3.1' from 'http://some.url.that.does.not.exist.com'", e.Message);
                Assert.Contains("Unable to load the service index for source", e.Message);
            }
        }

        [Fact]
        public void AdditionalAssemblies_NuGet_Live()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "HtmlIndex",
                    Maps =
                    {
                        "from c in docs.Companies select new { Name = typeof(HtmlAgilityPack.HtmlNode).Name }"
                    },
                    AdditionalAssemblies =
                    {
                        AdditionalAssembly.FromNuGet("HtmlAgilityPack", "1.11.28")
                    }
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);
            }
        }

        [Fact(Skip = "Uses ML.NET and downloads 350MB of packages")]
        public void CanUseMLNET()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "Employees/Tags",
                    Maps =
                    {
                        @"
from e in docs.Employees
let pic = LoadAttachment(e, ""photo.png"")
where pic != null
let classified =  ImageClassifier.Classify(pic.GetContentAsStream())
select new {
    e.Name,
    Tag = classified.Where(x => x.Value > 0.75f).Select(x => x.Key),
    _ = classified.Select(x => CreateField(x.Key, x.Value))
}
"
                    },
                    AdditionalSources = new System.Collections.Generic.Dictionary<string, string>
                    {
                        { "ImageClassifier", @"
using Microsoft.ML;
using Microsoft.ML.Data;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;

public static class ImageClassifier
{
    [ThreadStatic] private static PredictionEngine<ImageData, ImagePrediction> _predictor;
    [ThreadStatic] private static string[] _names;

    public static IDictionary<string, float> Classify(Stream s)
    {
        using var ms = new MemoryStream();
        s.CopyTo(ms);
        ms.Position = 0;
        Dictionary<string, float> results = new Dictionary<string, float>();

        _predictor ??= InitPredictor();

        using Bitmap bitmap = (Bitmap)Image.FromStream(ms);
        ImageData imageData = new ImageData { Image = bitmap };

        ImagePrediction prediction = _predictor.Predict(imageData);
        for (int i = 0; i < prediction.Score.Length; i++)
        {
            results[_names[i]] = prediction.Score[i];
        }
        return results;
    }

    private static PredictionEngine<ImageData, ImagePrediction> InitPredictor()
    {
        MLContext mlContext = new MLContext();
        ITransformer model = mlContext.Model.Load(""model.zip"", out _);
        PredictionEngine<ImageData, ImagePrediction> predictor =
            mlContext.Model.CreatePredictionEngine<ImageData, ImagePrediction>(model);
                VBuffer<ReadOnlyMemory<char>> slotNames = default;
                predictor.OutputSchema[nameof(ImagePrediction.Score)].GetSlotNames(ref slotNames);
                _names = slotNames.DenseValues().Select(x => x.ToString()).ToArray();
                return predictor;
            }

    public class ImageData
        {
            public Bitmap Image;
            public string Label;
        }

        public class ImagePrediction : ImageData
        {
            public string PredictedLabelValue;
            public float[] Score;
        }
    }
" }
                    },
                    AdditionalAssemblies =
                    {
                        AdditionalAssembly.FromRuntime("System.Memory"),
                        AdditionalAssembly.FromNuGet("System.Drawing.Common", "4.7.0"),
                        AdditionalAssembly.FromNuGet("Microsoft.ML", "1.5.2")
                    }
                }));
            }
        }
    }
}
