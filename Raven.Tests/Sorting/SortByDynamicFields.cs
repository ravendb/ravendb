using System;
using Lucene.Net.Index;
using Raven.Abstractions.Data;
using Raven.Database.Indexing.Sorting.Custom;

namespace Raven.Tests.Sorting
{
    public class SortByDynamicFields : IndexEntriesToComparablesGenerator
    {
        private int CustomTagId;
        public SortByDynamicFields(IndexQuery indexQuery) : base(indexQuery)
        {
            CustomTagId = IndexQuery.TransformerParameters["customTagId"].Value<int>();
        }
        

        public override IComparable Generate(IndexReader reader, int doc)
        {
            var ravenDoc = reader.Document(doc);
            
            var payingTagField = ravenDoc.GetField("PayingTag_" + CustomTagId);
            var queriedPayingTag = payingTagField != null && Boolean.Parse(payingTagField.StringValue);
            var tagValue = Int32.Parse(ravenDoc.GetField("TagId").StringValue);
            var pointsValue = Int32.Parse(ravenDoc.GetField("Points").StringValue);
            
            CustomerDocumentOrderWithRandomEffect.OrderCategory cat;
            
            if (tagValue == CustomTagId && queriedPayingTag )
            {
                cat = CustomerDocumentOrderWithRandomEffect.OrderCategory.TagAndPaying;
            }
            else if (queriedPayingTag)
            {
                cat = CustomerDocumentOrderWithRandomEffect.OrderCategory.OnlyPaying;
            }
            else if (tagValue == CustomTagId )
            {
                cat = CustomerDocumentOrderWithRandomEffect.OrderCategory.OnlyTag;
            }
            else
            {
                cat = CustomerDocumentOrderWithRandomEffect.OrderCategory.NoneOfTheAbove;
            }

            return new CustomerDocumentOrderWithRandomEffect()
            {
                Category = cat,
                Points = pointsValue
            };
        }


        public class CustomerDocumentOrderWithRandomEffect:IComparable
        {
            private Random rnd;

            public CustomerDocumentOrderWithRandomEffect()
            {
                rnd = new Random();
            }

            public enum OrderCategory
            {
                TagAndPaying = 4,
                OnlyPaying = 3,
                OnlyTag = 2,
                NoneOfTheAbove = 1
            }

            public OrderCategory Category;
            public int Points;

            public int CompareTo(object obj)
            {
                var compared = (CustomerDocumentOrderWithRandomEffect)obj;

                if (compared == null)
                    return 1;


                if (Category != compared.Category)
                    return Category > compared.Category ? 1 : -1;

                if (Points == compared.Points)
                    return rnd.Next(1000)<500?1:-1;


                return Points > compared.Points ? 1 : -1;
            }
        }
    }
}
