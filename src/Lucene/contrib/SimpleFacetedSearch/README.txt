SimpleFacetedSearch: Dynamic clustering of search results into categories according to values in given field(s). 
Its instances are tread-safe. So, the same instance can be shared among many threads like IndexReader.

Sample Usage:

    //Should be created only when IndexReader is opened/reopened. Creation with every search can be performance killer
    SimpleFacetedSearch sfs = new SimpleFacetedSearch(indexReader, new string[] { "source", "category" });

    Query query = new QueryParser(Lucene.Net.Util.Version.LUCENE_29, field, analyzer).Parse(searchString);
    SimpleFacetedSearch.Hits hits = sfs.Search(query, 10);
    
	long totalHits = hits.TotalHitCount;
	
    foreach (SimpleFacetedSearch.HitsPerFacet hpf in hits.HitsPerFacet)
    {
		long hitCountPerFacet = hpf.HitCount;
        SimpleFacetedSearch.FacetName name = hpf.Name;
		//name[0] 
		//name[1]
		//name.ToString()
		
        foreach (Document doc in hpf.Documents)
        {
             ........
        }
    }


