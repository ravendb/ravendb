using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Json.Parsing;

namespace Corax.Benchmark
{
    public class WikipediaLoader
    {
        private readonly DirectoryInfo _path;

        public WikipediaLoader(DirectoryInfo path)
        {
            this._path = path;
        }

        public IEnumerable<Tuple<string, DynamicJsonValue>> LoadAsDocuments()
        {
            foreach(var fileInfo in this._path.EnumerateFiles("20140615-wiki-en_*"))
            {
                using (var stream = new BufferedStream(fileInfo.OpenRead()))
                using (var reader = new StreamReader(stream))
                {
                    string name = string.Empty;
                    DynamicJsonValue textValue = null;
                    
                    var builder = new StringBuilder();                    

                    bool isFirst = true;
                    while ( !reader.EndOfStream )
                    {
                        var line = reader.ReadLine().Trim();
                        if (line.StartsWith("[[", StringComparison.Ordinal) && line.EndsWith("]]", StringComparison.Ordinal))
                        {
                            if (!isFirst)
                            {
                                textValue["Text"] = builder.ToString();

                                yield return new Tuple<string, DynamicJsonValue>(name, textValue);

                                builder.Clear();
                            }                                

                            textValue = new DynamicJsonValue();
                            name = line.Substring(2, line.Length - 4);                            

                            isFirst = false;
                        }
                        else
                        {
                            if (textValue == null)
                                continue;

                            builder.AppendLine(line);
                        }                                                    
                    }
                }
            }
        }
    }
}
