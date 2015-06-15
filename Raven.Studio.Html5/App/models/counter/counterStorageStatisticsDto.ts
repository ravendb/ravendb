interface counterStorageStatisticsDto {
    Name: string;
    Url: string;
    CountersCount: number;
    GroupsCount: number;
    LastCounterEtag:string;
    ReplicationTasksCount: number;
    CounterStorageSize: string;
    RequestsPerSecond: number;
}