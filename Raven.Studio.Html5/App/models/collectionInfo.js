define(["require", "exports", "models/document"], function(require, exports, document) {
    var collectionInfo = (function () {
        function collectionInfo(dto) {
            this.results = dto.Results.map(function (d) {
                return new document(d);
            });
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
        return collectionInfo;
    })();

    
    return collectionInfo;
});
//# sourceMappingURL=collectionInfo.js.map
