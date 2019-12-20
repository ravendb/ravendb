/// <reference path="../../../../typings/tsd.d.ts"/>
import genUtils = require("common/generalUtils");

class indexStatistics {
    isStale: boolean;
    indexName: string;
    indexType: Raven.Client.Documents.Indexes.IndexType;

    entriesCount: string; 
    errorsCount: string;  

    mapAttempts: string; 
    mapSuccesses: string;
    mapErrors: string;
    mapReferenceAttempts: string;
    mapReferenceSuccesses: string;
    mapReferenceErrors: string;
    
    showMapReferenceSection: boolean;
    
    mappedPerSecondRate: number; 
    mappedPerSecondRateStr: string;

    reduceAttempts?: string;        
    reduceSuccesses?: string;       
    reduceErrors?: string;          
    reducedPerSecondRate: number;
    reducedPerSecondRateStr: string;   

    isReduceIndex: boolean;
    isFaultyIndex: boolean;
    
    constructor(dto: Raven.Client.Documents.Indexes.IndexStats) {
        this.indexName = dto.Name;
        this.indexType = dto.Type;
        this.isStale = dto.IsStale;

        this.entriesCount = dto.EntriesCount.toLocaleString();
        this.errorsCount = dto.ErrorsCount > 0 ? dto.ErrorsCount.toLocaleString() : "0";

        this.mapAttempts = dto.MapAttempts.toLocaleString();
        this.mapSuccesses = dto.MapSuccesses.toLocaleString();
        this.mapErrors = dto.MapErrors > 0 ? dto.MapErrors.toLocaleString() : "0";
        
        this.mapReferenceAttempts = dto.MapReferenceAttempts > 0 ? dto.MapReferenceAttempts.toLocaleString() : "0";
        this.mapReferenceSuccesses = dto.MapReferenceSuccesses > 0 ? dto.MapReferenceSuccesses.toLocaleString() : "0";
        this.mapReferenceErrors = dto.MapReferenceErrors > 0 ? dto.MapReferenceErrors.toLocaleString() : "0";
        
        this.showMapReferenceSection = dto.MapReferenceSuccesses > 0 || dto.MapReferenceErrors > 0 || dto.MapReferenceAttempts > 0;

        this.mappedPerSecondRate = dto.MappedPerSecondRate;
        this.mappedPerSecondRateStr = dto.MappedPerSecondRate > 1 ? genUtils.formatNumberToStringFixed(dto.MappedPerSecondRate, 2) : "";
        
        this.reduceAttempts = dto.ReduceAttempts ? dto.ReduceAttempts.toLocaleString() : "";
        this.reduceSuccesses = dto.ReduceSuccesses ? dto.ReduceSuccesses.toLocaleString() : "";
        this.reduceErrors = dto.ReduceErrors > 0 ? dto.ReduceErrors.toLocaleString() : "0";

        this.reducedPerSecondRate = dto.ReducedPerSecondRate;
        this.reducedPerSecondRateStr = dto.ReducedPerSecondRate > 1 ? genUtils.formatNumberToStringFixed(dto.ReducedPerSecondRate, 2) : "";

        this.isReduceIndex = this.indexType === "AutoMapReduce" || this.indexType === "MapReduce" || this.indexType === "JavaScriptMapReduce";
        this.isFaultyIndex = this.indexType === "Faulty";
    }
}

export = indexStatistics;
