class abstractQueryResult {
	
	includes: any[]; 
	isStale: boolean;
	indexTimestamp: Date; 
	totalResults: number;
	skippedResults: number;
	indexName: string;
	indexEtag: string;
	resultEtag: string;
	highlightings: any;
	nonAuthoritativeInformation: boolean;
	lastQueryTime: Date;
	durationMilliseconds: number;

	constructor(dto: collectionInfoDto) {
		this.includes = dto.Includes;
		this.isStale = dto.IsStale;
		this.indexTimestamp = new Date(dto.IndexTimestamp);
		this.totalResults = dto.TotalResults;
		this.skippedResults = dto.SkippedResults;
		this.indexName = dto.IndexName;
		this.indexEtag = dto.IndexEtag;
		this.resultEtag = dto.ResultEtag;
		this.highlightings = dto.Highlightings;
		this.nonAuthoritativeInformation = dto.NonAuthoritativeInformation;
		this.lastQueryTime = new Date(dto.LastQueryTime);
		this.durationMilliseconds = dto.DurationMilliseconds;
	}
}

export = abstractQueryResult;