using System.Collections.Generic;
using System.IO;
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
                        "from c in docs.Companies select new { Name = typeof(HtmlAgilityPack.HtmlNode).Assembly.FullName }"
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

                var terms = store.Maintenance.Send(new GetTermsOperation("HtmlIndex", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("1.11.28.0", terms[0]);

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "HtmlIndex",
                    Maps =
                    {
                        "from c in docs.Companies select new { Name = typeof(HtmlAgilityPack.HtmlNode).Assembly.FullName }"
                    },
                    AdditionalAssemblies =
                    {
                        AdditionalAssembly.FromNuGet("HtmlAgilityPack", "1.11.32")
                    }
                }));

                WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                terms = store.Maintenance.Send(new GetTermsOperation("HtmlIndex", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("1.11.32.0", terms[0]);

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "HtmlIndex_2",
                    Maps =
                    {
                        "from c in docs.Companies select new { Name = typeof(HtmlAgilityPack.HtmlNode).Assembly.FullName }"
                    },
                    AdditionalAssemblies =
                    {
                        AdditionalAssembly.FromNuGet("HtmlAgilityPack", "1.11.28")
                    }
                }));

                WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                terms = store.Maintenance.Send(new GetTermsOperation("HtmlIndex_2", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("1.11.28.0", terms[0]);
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

        [Fact(Skip = "Uses ML.NET and downloads 350MB of packages")]
        public void CanUseMLNET_Omnx()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "Employees/Tags",
                    Maps =
                    {
                        @"
from i in docs.Employees
let pic = LoadAttachment(i, ""photo.png"")
let classified = ObjectClassification.GetObjects(pic.GetContentAsStream())
select new 
{
    Tags = classified.Keys,
    Count = classified.Count
}
"
                    },
                    AdditionalSources = new System.Collections.Generic.Dictionary<string, string>
                    {
                        { "ImageClassifier", @"
using System;
using System.IO;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using Microsoft.ML;
using Microsoft.ML.Data;
using Microsoft.ML.Transforms.Image;
using Microsoft.ML.Transforms.Onnx;
using System.Net.Http;

namespace ObjectDetection
{
    public class ObjectClassification
    {
        public const int ROW_COUNT = 13;
        public const int COL_COUNT = 13;
        public const int BOXES_PER_CELL = 5;
        public const int CLASS_COUNT = 20;
        public const int BOX_INFO_FEATURE_COUNT = 5;
        private const int channelStride = ROW_COUNT * COL_COUNT;
        private static string[] labels = new string[]
        {
            ""aeroplane"", ""bicycle"", ""bird"", ""boat"", ""bottle"",
            ""bus"", ""car"", ""cat"", ""chair"", ""cow"",
            ""diningtable"", ""dog"", ""horse"", ""motorbike"", ""person"",
            ""pottedplant"", ""sheep"", ""sofa"", ""train"", ""tvmonitor""
        };

        [ThreadStatic]
        private static Context _context;

        public class Context
        {
            public TransformerChain<OnnxTransformer> Model;
            public MLContext MLContext;
            public MemoryStream Mem = new MemoryStream();
            public InMemoryImageData[] Array = new InMemoryImageData[1]
            {
                new InMemoryImageData()
            };
            private struct ImageNetSettings
            {
                public const int imageHeight = 416;
                public const int imageWidth = 416;
            }

            public class InMemoryImageData
            {
                [ImageType(ImageNetSettings.imageHeight, ImageNetSettings.imageWidth)]
                [LoadColumn(0)]
                public Bitmap Image;
            }

            private static string DownloadCache(string url)
            {
                var file = Path.GetFileName(new Uri(url).AbsolutePath);
                var cache = Path.Combine(Path.GetTempPath(), file);
                if (File.Exists(cache))
                    return cache;

                try
                {
                    using var client = new HttpClient();
                    using var f = File.Create(cache);
                    client.GetAsync(url).Result.Content.ReadAsStreamAsync().Result.CopyTo(f);
                    return cache;
                }
                catch (Exception)
                {
                    File.Delete(cache);//cleanup on failure
                    throw;
                }
            }
            
            public Context()
            {
                var modelFilePath = DownloadCache(""https://media.githubusercontent.com/media/onnx/models/master/vision/object_detection_segmentation/tiny-yolov2/model/tinyyolov2-7.onnx"");
                MLContext = new MLContext();
                var pipeline = MLContext.Transforms.ResizeImages(outputColumnName: ""image"", imageWidth: ImageNetSettings.imageWidth, imageHeight: ImageNetSettings.imageHeight, inputColumnName: ""Image"")
                                .Append(MLContext.Transforms.ExtractPixels(outputColumnName: ""image""))
                                .Append(MLContext.Transforms.ApplyOnnxModel(""grid"", ""image"", modelFilePath));
                var data = MLContext.Data.LoadFromEnumerable(new List<InMemoryImageData>());
                Model = pipeline.Fit(data);
            }
        }

        public static dynamic GetObjects(Stream s)
        {
            _context ??= new Context();
            _context.Mem.SetLength(0);
            s.CopyTo(_context.Mem);
            _context.Mem.Position = 0;
            using var bitmap = (Bitmap)Bitmap.FromStream(_context.Mem);
            _context.Array[0].Image = bitmap;

            var scoredData = _context.Model.Transform(_context.MLContext.Data.LoadFromEnumerable(_context.Array));
            var matches = new Dictionary<string, float>();
            foreach (var prob in scoredData.GetColumn<float[]>(TinyYoloModelSettings.ModelOutput))
            {
                ParseOutputs(prob, matches);
            }
            _context.Array[0].Image = null;
            return matches;
        }

        private static void ParseOutputs(float[] yoloModelOutputs, Dictionary<string, float> matches, float threshold = .3F)
        {
            for (int row = 0; row < ROW_COUNT; row++)
            {
                for (int column = 0; column < COL_COUNT; column++)
                {
                    for (int box = 0; box < BOXES_PER_CELL; box++)
                    {
                        var channel = (box * (CLASS_COUNT + BOX_INFO_FEATURE_COUNT));

                        float confidence = GetConfidence(yoloModelOutputs, row, column, channel);

                        if (confidence < threshold)
                            continue;

                        float[] predictedClasses = ExtractClasses(yoloModelOutputs, row, column, channel);

                        var (topResultIndex, topResultScore) = GetTopResult(predictedClasses);
                        var topScore = topResultScore * confidence;

                        if (topScore < threshold)
                            continue;

                        if(matches.TryGetValue(labels[topResultIndex], out var f) == false || f < topResultScore)
                        {
                            matches[labels[topResultIndex]] = topResultScore;
                        }
                    }
                }
            }
        }

        private static float GetConfidence(float[] modelOutput, int x, int y, int channel)
        {
            return Sigmoid(modelOutput[GetOffset(x, y, channel + 4)]);
        }

        private static float Sigmoid(float value)
        {
            var k = (float)Math.Exp(value);
            return k / (1.0f + k);
        }

        private static int GetOffset(int x, int y, int channel)
        {
            // YOLO outputs a tensor that has a shape of 125x13x13, which 
            // WinML flattens into a 1D array.  To access a specific channel 
            // for a given (x,y) cell position, we need to calculate an offset
            // into the array
            return (channel * channelStride) + (y * COL_COUNT) + x;
        }

        private static float[] Softmax(float[] values)
        {
            var maxVal = values.Max();
            var exp = values.Select(v => Math.Exp(v - maxVal));
            var sumExp = exp.Sum();

            return exp.Select(v => (float)(v / sumExp)).ToArray();
        }

        private static float[] ExtractClasses(float[] modelOutput, int x, int y, int channel)
        {
            float[] predictedClasses = new float[CLASS_COUNT];
            int predictedClassOffset = channel + BOX_INFO_FEATURE_COUNT;
            for (int predictedClass = 0; predictedClass < CLASS_COUNT; predictedClass++)
            {
                predictedClasses[predictedClass] = modelOutput[GetOffset(x, y, predictedClass + predictedClassOffset)];
            }
            return Softmax(predictedClasses);
        }

        private static ValueTuple<int, float> GetTopResult(float[] predictedClasses)
        {
            return predictedClasses
                .Select((predictedClass, index) => (Index: index, Value: predictedClass))
                .OrderByDescending(result => result.Value)
                .First();
        }

        public struct TinyYoloModelSettings
        {
            // for checking Tiny yolo2 Model input and  output  parameter names,
            //you can use tools like Netron, 
            // which is installed by Visual Studio AI Tools

            // input tensor name
            public const string ModelInput = ""image"";

            // output tensor name
            public const string ModelOutput = ""grid"";
        }
    }
}
" }
                    },
                    AdditionalAssemblies =
                    {
                        AdditionalAssembly.FromRuntime("System.IO.FileSystem"),
                        AdditionalAssembly.FromRuntime("System.Net.Http"),
                        AdditionalAssembly.FromNuGet("Microsoft.ML", "1.5.5"),
                        AdditionalAssembly.FromNuGet("Microsoft.ML.ImageAnalytics", "1.5.5"),
                        AdditionalAssembly.FromNuGet("Microsoft.ML.OnnxTransformer", "1.5.5"),
                        AdditionalAssembly.FromNuGet("Microsoft.ML.OnnxRuntime.Managed", "1.7.1"),
                        AdditionalAssembly.FromNuGet("Microsoft.ML.OnnxRuntime", "1.7.0")
                    }
                }));

                using (var session = store.OpenSession())
                {
                    var employee = new Employee { FirstName = "John", LastName = "Doe" };

                    session.Store(employee);

                    var image = GetImage("RavenDB_15753.cat.jpg"); // from: https://unsplash.com/photos/IuJc2qh2TcA
                    session.Advanced.Attachments.Store(employee, "photo.png", image);

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);
            }
        }

        private static Stream GetImage(string name)
        {
            var assembly = typeof(RavenDB_15753).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }
    }
}
