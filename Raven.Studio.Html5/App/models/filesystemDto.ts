interface filesystemStatisticsDto {
    Name: string;
    FileCount: number;
}

interface filesystemFileHeaderDto {
    Name: string;

    TotalSize?: number;
    UploadedSize: number;
		
    HumaneTotalSize: string;
    HumaneUploadedSize: string;

    Metadata: any;
}